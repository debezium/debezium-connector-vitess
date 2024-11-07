/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.vitess.connection.DdlMetadataExtractor;
import io.debezium.connector.vitess.connection.ReplicationConnection;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.ReplicationMessageProcessor;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessTransactionInfo;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;

/**
 * Read events from source and dispatch each event using {@link EventDispatcher} to the {@link
 * io.debezium.pipeline.source.spi.ChangeEventSource}. It runs in the
 * change-event-source-coordinator thread only.
 */
public class VitessStreamingChangeEventSource implements StreamingChangeEventSource<VitessPartition, VitessOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessStreamingChangeEventSource.class);

    private final EventDispatcher<VitessPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final VitessDatabaseSchema schema;
    private final VitessConnectorConfig connectorConfig;
    private final ReplicationConnection replicationConnection;
    private final DelayStrategy pauseNoMessage;

    public VitessStreamingChangeEventSource(
                                            EventDispatcher<VitessPartition, TableId> dispatcher,
                                            ErrorHandler errorHandler,
                                            Clock clock,
                                            VitessDatabaseSchema schema,
                                            VitessConnectorConfig connectorConfig,
                                            ReplicationConnection replicationConnection) {
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.connectorConfig = connectorConfig;
        this.replicationConnection = replicationConnection;
        this.pauseNoMessage = DelayStrategy.constant(connectorConfig.getPollInterval());

        LOGGER.info("VitessStreamingChangeEventSource is created");
    }

    @Override
    public void execute(ChangeEventSourceContext context, VitessPartition partition, VitessOffsetContext offsetContext) {
        if (offsetContext == null) {
            offsetContext = VitessOffsetContext.initialContext(connectorConfig, clock);
        }

        try {
            AtomicReference<Throwable> error = new AtomicReference<>();
            replicationConnection.startStreaming(
                    offsetContext.getRestartVgtid(), newReplicationMessageProcessor(partition, offsetContext), error);

            while (context.isRunning() && error.get() == null) {
                pauseNoMessage.sleepWhen(true);
            }
            if (error.get() != null) {
                LOGGER.error("Error during streaming", error.get());
                throw error.get();
            }
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            try {
                // closing the connection should also disconnect the VStream gRPC channel
                replicationConnection.close();
            }
            catch (Exception e) {
                LOGGER.error("Failed to close replicationConnection", e);
            }
        }
    }

    private ReplicationMessageProcessor newReplicationMessageProcessor(VitessPartition partition,
                                                                       VitessOffsetContext offsetContext) {
        return (message, newVgtid, isLastRowOfTransaction) -> {
            if (message.isTransactionalMessage()) {
                // Tx BEGIN/END event
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
                if (message.getOperation() == ReplicationMessage.Operation.BEGIN) {
                    // send to transaction topic
                    VitessTransactionInfo transactionInfo = new VitessTransactionInfo(message.getTransactionId(), message.getShard());
                    dispatcher.dispatchTransactionStartedEvent(partition, transactionInfo, offsetContext, message.getCommitTime());
                }
                else if (message.getOperation() == ReplicationMessage.Operation.COMMIT) {
                    // send to transaction topic
                    dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, message.getCommitTime());
                    // Send a heartbeat event if time has elapsed
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
                return;
            }
            else if (message.getOperation() == ReplicationMessage.Operation.OTHER) {
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
            }
            else if (message.getOperation() == ReplicationMessage.Operation.DDL) {
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
                offsetContext.setShard(message.getShard());

                DdlMetadataExtractor metadataExtractor = new DdlMetadataExtractor(message);
                TableId tableId = VitessDatabaseSchema.parse(metadataExtractor.getTable());
                offsetContext.event(tableId, message.getCommitTime());
                String ddlStatement = message.getStatement();
                SchemaChangeEvent.SchemaChangeEventType eventType = metadataExtractor.getSchemaChangeEventType();
                SchemaChangeEvent schemaChangeEvent = SchemaChangeEvent.of(
                        eventType,
                        partition,
                        offsetContext,
                        connectorConfig.getKeyspace(),
                        null,
                        ddlStatement,
                        null,
                        false);
                dispatcher.dispatchSchemaChangeEvent(partition, offsetContext, null, (receiver) -> {
                    try {
                        receiver.schemaChangeEvent(schemaChangeEvent);
                    }
                    catch (Exception e) {
                        throw new DebeziumException(e);
                    }
                });
            }
            else if (message.getOperation().equals(ReplicationMessage.Operation.HEARTBEAT)) {
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                // DML event
                TableId tableId = VitessDatabaseSchema.parse(message.getTable());
                Objects.requireNonNull(tableId);

                offsetContext.event(tableId, message.getCommitTime());
                offsetContext.setShard(message.getShard());
                if (isLastRowOfTransaction) {
                    // Right before processing the last row, reset the previous offset to the new vgtid so the last row has the new vgtid as offset.
                    offsetContext.resetVgtid(newVgtid, message.getCommitTime());
                }
                dispatcher.dispatchDataChangeEvent(
                        partition,
                        tableId,
                        new VitessChangeRecordEmitter(
                                partition, offsetContext, clock, connectorConfig, schema, message));
            }
        };
    }
}
