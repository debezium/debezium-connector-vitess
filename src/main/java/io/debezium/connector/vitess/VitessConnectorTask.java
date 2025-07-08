/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.common.DebeziumHeaderProducerProvider;
import io.debezium.connector.vitess.connection.ReplicationConnection;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.connector.vitess.metrics.VitessChangeEventSourceMetricsFactory;
import io.debezium.converters.custom.CustomConverterServiceProvider;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.service.spi.ServiceRegistry;
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
        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        CustomConverterRegistry customConverterRegistry = connectorConfig.getServiceRegistry().tryGetService(CustomConverterRegistry.class);

        schema = new VitessDatabaseSchema(connectorConfig, schemaNameAdjuster, topicNamingStrategy, customConverterRegistry);
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
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        try {
            if (previousOffset == null) {
                LOGGER.info("No previous offset found");
            }
            else {
                LOGGER.info("Found task {} previous offset {}", config.getString(ConfigurationNames.TASK_ID_PROPERTY_NAME), previousOffset);
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
                    new VitessHeartbeatFactory()
                            .getScheduledHeartbeat(connectorConfig, null, null, queue),
                    schemaNameAdjuster,
                    connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

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

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @VisibleForTesting
    public Configuration getConfigWithOffsets(Configuration config) {
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        if (connectorConfig.offsetStoragePerTask()) {
            Object vgtid = getVitessTaskValuePerShard(connectorConfig, OffsetValueType.GTID);
            config = config.edit().with(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG, vgtid).build();
            if (VitessOffsetRetriever.isShardEpochMapEnabled(connectorConfig)) {
                Object shardEpochMap = getVitessTaskValuePerShard(connectorConfig, OffsetValueType.EPOCH);
                config = config.edit().with(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG, shardEpochMap).build();
            }
        }
        return config;
    }

    private Object getVitessTaskValuePerShard(VitessConnectorConfig connectorConfig, OffsetValueType valueType) {
        int gen = connectorConfig.getOffsetStorageTaskKeyGen();
        int prevGen = gen - 1;
        VitessOffsetRetriever prevGenRetriever = new VitessOffsetRetriever(
                connectorConfig,
                connectorConfig.getPrevNumTasks(),
                prevGen,
                true,
                context.offsetStorageReader());
        Map<String, ?> prevGenValuesPerShard = prevGenRetriever.getValuePerShardFromStorage(valueType);
        LOGGER.info("{} per shard: {}", valueType.name(), prevGenValuesPerShard);
        VitessOffsetRetriever retriever = new VitessOffsetRetriever(
                connectorConfig,
                connectorConfig.getVitessTotalTasksConfig(),
                gen,
                false,
                context.offsetStorageReader());
        Map<String, ?> curGenValuesPerShard = retriever.getValuePerShardFromStorage(valueType);
        LOGGER.info("{} per shard {}", valueType.name(), curGenValuesPerShard);
        List<String> shards = connectorConfig.getVitessTaskKeyShards();
        Map<String, Object> configValuesPerShard = valueType.configValuesFunction.apply(connectorConfig, shards);
        LOGGER.info("config {} per shard {}", valueType.name(), configValuesPerShard);
        final String keyspace = connectorConfig.getKeyspace();

        Map<String, Object> valuesPerShard = new TreeMap();
        for (String shard : shards) {
            Object value;
            if (curGenValuesPerShard != null && curGenValuesPerShard.containsKey(shard)) {
                value = curGenValuesPerShard.get(shard);
                LOGGER.info("Using offsets from current gen: shard {}, gen {}, {} {}", shard, gen, valueType.name(), value);
            }
            else if (prevGenValuesPerShard != null && prevGenValuesPerShard.containsKey(shard)) {
                value = prevGenValuesPerShard.get(shard);
                LOGGER.warn("Using offsets from previous gen: shard {}, gen {}, {} {}", shard, gen, valueType.name(), value);
            }
            else {
                value = configValuesPerShard.getOrDefault(shard, null);
                LOGGER.warn("Using offsets from config: shard {}, {} {}", shard, valueType.name(), valueType.name(), value);
            }
            valuesPerShard.put(shard, value);
        }
        return valueType.conversionFunction.apply(valuesPerShard, keyspace);
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        final List<SourceRecord> sourceRecords = records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
        return sourceRecords;
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.of(errorHandler);
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

    // Remove when support for SPI snapshotter will be implemented with DBZ-7307
    @Override
    protected void registerServiceProviders(ServiceRegistry serviceRegistry) {

        serviceRegistry.registerServiceProvider(new PostProcessorRegistryServiceProvider());
        serviceRegistry.registerServiceProvider(new DebeziumHeaderProducerProvider());
        serviceRegistry.registerServiceProvider(new CustomConverterServiceProvider());
    }
}
