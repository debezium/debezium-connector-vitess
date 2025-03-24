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
        return (message, newVgtid) -> {
            if (message.isTransactionalMessage()) {
                // Tx BEGIN/COMMIT event
                if (message.getOperation() == ReplicationMessage.Operation.BEGIN) {
                    // When BEGIN event is received, newVgtid will have been populated with the transaction's vgtid, rotate
                    // to set currentVgtid to newVgtid and restartVgtid to the previous transaction VGTID
                    offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
                    // send to transaction topic
                    VitessTransactionInfo transactionInfo = new VitessTransactionInfo(message.getTransactionId(), message.getShard());
                    dispatcher.dispatchTransactionStartedEvent(partition, transactionInfo, offsetContext, message.getCommitTime());
                }
                else {
                    // When COMMIT event is received, all events have been processed except for this COMMIT event
                    // We reset the VGTID such that current & restart VGTIDs are equal to this transaction's VGTID
                    // We send one final event (transaction committed), the offset will only be committed if that event
                    // is sent successfully.
                    // If transaction metadata is disabled, then the offset will not be updated until a message of the next
                    // transaction is sent (next transaction's restartVgtid = this transaction's currentVgtid)
                    offsetContext.resetVgtid(newVgtid, message.getCommitTime());
                    // send to transaction topic
                    dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, message.getCommitTime());
                    // Send a heartbeat event if time has elapsed
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            else if (message.getOperation() == ReplicationMessage.Operation.OTHER) {
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
            }
            else if (message.getOperation() == ReplicationMessage.Operation.DDL) {
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
                offsetContext.setShard(message.getShard());

                // DDLs events are only published if the schema change history is enabled, so we should skip parsing DDL events if it's disabled
                if (connectorConfig.isSchemaChangesHistoryEnabled()) {
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
                dispatcher.dispatchDataChangeEvent(
                        partition,
                        tableId,
                        new VitessChangeRecordEmitter(
                                partition, offsetContext, clock, connectorConfig, schema, message));
            }
        };
    }
}
