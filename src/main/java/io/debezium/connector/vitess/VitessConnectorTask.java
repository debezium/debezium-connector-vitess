/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.vitess.connection.ReplicationConnection;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.SchemaNameAdjuster;

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

        final VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = VitessTopicSelector.defaultSelector(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        schema = new VitessDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector);
        VitessTaskContext taskContext = new VitessTaskContext(connectorConfig, schema);
        Offsets<VitessPartition, VitessOffsetContext> previousOffsets = getPreviousOffsets(new VitessPartition.Provider(connectorConfig),
                new VitessOffsetContext.Loader(connectorConfig));
        final VitessOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();
        final Clock clock = Clock.system();

        // Mapped Diagnostic Context (MDC) logging
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);

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
            errorHandler = new ErrorHandler(VitessConnector.class, connectorConfig.getLogicalName(), queue);

            // for metrics
            final VitessEventMetadataProvider metadataProvider = new VitessEventMetadataProvider();

            final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    new Filters(connectorConfig).tableFilter(),
                    DataChangeEvent::new,
                    metadataProvider,
                    schemaNameAdjuster);

            ChangeEventSourceCoordinator<VitessPartition, VitessOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                    previousOffsets,
                    errorHandler,
                    VitessConnector.class,
                    connectorConfig,
                    new VitessChangeEventSourceFactory(
                            connectorConfig, errorHandler, dispatcher, clock, schema, replicationConnection),
                    new DefaultChangeEventSourceMetricsFactory(),
                    dispatcher,
                    schema);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
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
