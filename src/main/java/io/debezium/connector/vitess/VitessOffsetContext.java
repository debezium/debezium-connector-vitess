/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Current offset of the connector. There is only one instance per connector setup. We need to
 * update the offset by calling the APIs provided by this class, every time we process a new
 * ReplicationMessage.
 */
public class VitessOffsetContext implements OffsetContext {
    private static final String SERVER_PARTITION_KEY = "server";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private final TransactionContext transactionContext;

    // Only used when resuming from the previous offset
    private long initialEventsToSkip = 0L;
    private boolean skipEvent = false;

    public VitessOffsetContext(
                               VitessConnectorConfig connectorConfig,
                               Vgtid initialVgtid,
                               long restartEventsToSkip,
                               Instant time,
                               TransactionContext transactionContext) {
        this.partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.sourceInfo = new SourceInfo(connectorConfig);
        this.sourceInfo.initialVgtid(initialVgtid, time);
        this.sourceInfoSchema = sourceInfo.schema();
        this.transactionContext = transactionContext;
        this.initialEventsToSkip = restartEventsToSkip;
        this.skipEvent = initialEventsToSkip > 0;
    }

    /** Initialize VitessOffsetContext if no previous offset exists */
    public static VitessOffsetContext initialContext(
                                                     VitessConnectorConfig connectorConfig, Clock clock) {
        final Vgtid defaultVgtid = VitessReplicationConnection.defaultVgtid(connectorConfig);
        return new VitessOffsetContext(
                connectorConfig, defaultVgtid, 0L, clock.currentTimeAsInstant(), new TransactionContext());
    }

    public void startRowEvent(Instant commitTime, TableId tableId) {
        if (skipEvent) {
            initialEventsToSkip--;
            skipEvent = initialEventsToSkip > 0;
        }
        sourceInfo.startRowEvent(commitTime, tableId);
    }

    /**
     * Rotate current and restart vgtid. Only rotate wen necessary.
     */
    public void rotateVgtid(Vgtid newVgtid, Instant commitTime) {
        sourceInfo.rotateVgtid(newVgtid, commitTime);
    }

    public Vgtid getRestartVgtid() {
        return sourceInfo.getRestartVgtid();
    }

    public long getRestartEventsToSkip() {
        return sourceInfo.getRestartEventsToSkip();
    }

    public long getInitialEventsToSkip() {
        return initialEventsToSkip;
    }

    public boolean isSkipEvent() {
        return skipEvent;
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    /**
     * Calculate and return the offset that will be used to create the {@link SourceRecord}.
     *
     * @return
     */
    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        if (sourceInfo.getRestartVgtid() != null) {
            if (sourceInfo.getRestartVgtid().getKeyspace() != null) {
                result.put(SourceInfo.VGTID_KEYSPACE, sourceInfo.getRestartVgtid().getKeyspace());
            }
            if (sourceInfo.getRestartVgtid().getShard() != null) {
                result.put(SourceInfo.VGTID_SHARD, sourceInfo.getRestartVgtid().getShard());
            }
            if (sourceInfo.getRestartVgtid().getGtid() != null) {
                result.put(SourceInfo.VGTID_GTID, sourceInfo.getRestartVgtid().getGtid());
            }
        }
        result.put(SourceInfo.EVENTS_TO_SKIP, sourceInfo.getRestartEventsToSkip());
        // put OFFSET_TRANSACTION_ID
        return transactionContext.store(result);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return false;
    }

    @Override
    public void markLastSnapshotRecord() {
    }

    @Override
    public void preSnapshotStart() {
    }

    @Override
    public void preSnapshotCompletion() {
    }

    @Override
    public void postSnapshotCompletion() {
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setTimestamp(timestamp);
        sourceInfo.setTableId((TableId) collectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public String toString() {
        return "VitessOffsetContext{"
                + "sourceInfo="
                + sourceInfo
                + ", partition="
                + partition
                + ", initialEventsToSkip="
                + initialEventsToSkip
                + ", skipEvent="
                + skipEvent
                + '}';
    }

    public static class Loader implements OffsetContext.Loader {

        private final VitessConnectorConfig connectorConfig;

        public Loader(VitessConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        }

        @Override
        public OffsetContext load(Map<String, ?> offset) {
            final String keyspace = (String) offset.get(SourceInfo.VGTID_KEYSPACE);
            final String shard = (String) offset.get(SourceInfo.VGTID_SHARD);
            final String gtid = (String) offset.get(SourceInfo.VGTID_GTID);
            final Long restartEventsToSkip = (Long) offset.get(SourceInfo.EVENTS_TO_SKIP);
            return new VitessOffsetContext(
                    connectorConfig,
                    Vgtid.of(keyspace, shard, gtid),
                    restartEventsToSkip,
                    null,
                    TransactionContext.load(offset));
        }
    }
}
