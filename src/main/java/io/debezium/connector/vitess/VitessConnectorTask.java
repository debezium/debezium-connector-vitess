/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.vitess.connection.ReplicationConnection;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.connector.vitess.metrics.VitessChangeEventSourceMetricsFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/** The main task executing streaming from Vitess. */
public class VitessConnectorTask extends BaseSourceTask<VitessPartition, VitessOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnectorTask.class);
    private static final String CONTEXT_NAME = "vitess-connector-task";

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile ErrorHandler errorHandler;
    private volatile VitessDatabaseSchema schema;
    private volatile ReplicationConnection replicationConnection;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected ChangeEventSourceCoordinator<VitessPartition, VitessOffsetContext> start(Configuration config) {

        config = getConfigWithOffsets(config);
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);

        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        schema = new VitessDatabaseSchema(connectorConfig, schemaNameAdjuster, topicNamingStrategy);
        VitessTaskContext taskContext = new VitessTaskContext(connectorConfig, schema);
        Offsets<VitessPartition, VitessOffsetContext> previousOffsets = getPreviousOffsets(new VitessPartition.Provider(connectorConfig),
                new VitessOffsetContext.Loader(connectorConfig));
        final VitessOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();
        final Clock clock = Clock.system();

        // Mapped Diagnostic Context (MDC) logging
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);

        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        try {
            if (previousOffset == null) {
                LOGGER.info("No previous offset found");
            }
            else {
                LOGGER.info("Found previous offset {}", previousOffset);
            }

            replicationConnection = new VitessReplicationConnection(connectorConfig, schema);

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            // saves the exception in the ChangeEventQueue, later task poll() would throw the exception
            errorHandler = new VitessErrorHandler(connectorConfig, queue, errorHandler);

            // for metrics
            final VitessEventMetadataProvider metadataProvider = new VitessEventMetadataProvider();

            final EventDispatcher<VitessPartition, TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    new Filters(connectorConfig).tableFilter(),
                    DataChangeEvent::new,
                    metadataProvider,
                    schemaNameAdjuster);

            NotificationService<VitessPartition, VitessOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            ChangeEventSourceCoordinator<VitessPartition, VitessOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                    previousOffsets,
                    errorHandler,
                    VitessConnector.class,
                    connectorConfig,
                    new VitessChangeEventSourceFactory(
                            connectorConfig, errorHandler, dispatcher, clock, schema, replicationConnection, snapshotterService),
                    connectorConfig.offsetStoragePerTask() ? new VitessChangeEventSourceMetricsFactory() : new DefaultChangeEventSourceMetricsFactory<>(),
                    dispatcher,
                    schema,
                    null,
                    notificationService,
                    snapshotterService);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    @VisibleForTesting
    protected Configuration getConfigWithOffsets(Configuration config) {
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        if (connectorConfig.offsetStoragePerTask()) {
            int gen = connectorConfig.getOffsetStorageTaskKeyGen();
            int prevGen = gen - 1;
            Map<String, String> prevGtidsPerShard = VitessConnector.getGtidPerShardFromStorage(
                    context.offsetStorageReader(),
                    connectorConfig,
                    connectorConfig.getPrevNumTasks(),
                    prevGen,
                    true);
            LOGGER.info("prevGtidsPerShard {}", prevGtidsPerShard);
            Map<String, String> gtidsPerShard = VitessConnector.getGtidPerShardFromStorage(
                    context.offsetStorageReader(),
                    connectorConfig,
                    connectorConfig.getVitessTotalTasksConfig(),
                    gen,
                    false);
            LOGGER.info("gtidsPerShard {}", gtidsPerShard);
            List<String> shards = connectorConfig.getVitessTaskKeyShards();
            Map<String, String> configGtidsPerShard = getConfigGtidsPerShard(connectorConfig, shards);
            LOGGER.info("configGtidsPerShard {}", configGtidsPerShard);
            final String keyspace = connectorConfig.getKeyspace();

            List<Vgtid.ShardGtid> shardGtids = new ArrayList<>();
            for (String shard : shards) {
                String gtidStr;
                if (gtidsPerShard.containsKey(shard)) {
                    gtidStr = gtidsPerShard.get(shard);
                    LOGGER.info("Using offsets from current gen: shard {}, gen {}, gtid {}", shard, gen, gtidStr);
                }
                else if (prevGtidsPerShard.containsKey(shard)) {
                    gtidStr = prevGtidsPerShard.get(shard);
                    LOGGER.warn("Using offsets from previous gen: shard {}, gen {}, gtid {}", shard, gen, gtidStr);
                }
                else {
                    gtidStr = configGtidsPerShard.getOrDefault(shard, null);
                    LOGGER.warn("Using offsets from config: shard {}, gtid {}", shard, gtidStr);
                }
                shardGtids.add(new Vgtid.ShardGtid(keyspace, shard, gtidStr));
            }
            return config.edit().with(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG, Vgtid.of(shardGtids)).build();
        }
        else {
            return config;
        }
    }

    private static Map<String, String> getConfigGtidsPerShard(VitessConnectorConfig connectorConfig, List<String> shards) {
        String gtids = connectorConfig.getVgtid();
        Map<String, String> configGtidsPerShard = null;
        if (shards != null && gtids.equals(Vgtid.EMPTY_GTID)) {
            Function<Integer, String> emptyGtid = x -> Vgtid.EMPTY_GTID;
            configGtidsPerShard = buildMap(shards, emptyGtid);
        }
        else if (shards != null && gtids.equals(Vgtid.CURRENT_GTID)) {
            Function<Integer, String> currentGtid = x -> Vgtid.CURRENT_GTID;
            configGtidsPerShard = buildMap(shards, currentGtid);
        }
        else if (shards != null) {
            List<Vgtid.ShardGtid> shardGtids = Vgtid.of(gtids).getShardGtids();
            Map<String, String> shardsToGtid = new HashMap<>();
            for (Vgtid.ShardGtid shardGtid : shardGtids) {
                shardsToGtid.put(shardGtid.getShard(), shardGtid.getGtid());
            }
            Function<Integer, String> shardGtid = (i -> shardsToGtid.get(shards.get(i)));
            configGtidsPerShard = buildMap(shards, shardGtid);
        }
        LOGGER.info("Found GTIDs per shard in config {}", configGtidsPerShard);
        return configGtidsPerShard;
    }

    private static Map<String, String> buildMap(List<String> keys, Function<Integer, String> function) {
        return IntStream.range(0, keys.size())
                .boxed()
                .collect(Collectors.toMap(keys::get, function));
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        final List<SourceRecord> sourceRecords = records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
        return sourceRecords;
    }

    @Override
    protected void doStop() {
        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return VitessConnectorConfig.ALL_FIELDS;
    }
}
