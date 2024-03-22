/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.connector.vitess.transaction.OrderedTransactionContext;
import io.debezium.connector.vitess.transaction.VitessEpochProvider;
import io.debezium.connector.vitess.transaction.VitessRankProvider;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Current offset of the connector. There is only one instance per connector setup. We need to
 * update the offset by calling the APIs provided by this class, every time we process a new
 * ReplicationMessage.
 */
public class VitessOffsetContext extends CommonOffsetContext<SourceInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessOffsetContext.class);

    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;

    public VitessOffsetContext(
                               VitessConnectorConfig connectorConfig,
                               Vgtid initialVgtid,
                               Instant time,
                               TransactionContext transactionContext) {
        super(new SourceInfo(connectorConfig));
        this.sourceInfo.resetVgtid(initialVgtid, time);
        this.sourceInfoSchema = sourceInfo.schema();
        this.transactionContext = transactionContext;
    }

    /** Initialize VitessOffsetContext if no previous offset exists */
    public static VitessOffsetContext initialContext(
                                                     VitessConnectorConfig connectorConfig, Clock clock) {
        LOGGER.info("No previous offset exists. Use default VGTID.");
        final Vgtid defaultVgtid = VitessReplicationConnection.defaultVgtid(connectorConfig);
        // use the other transaction context
        TransactionContext transactionContext;
        if (connectorConfig.shouldProvideOrderedTransactionMetadata()) {
            transactionContext = new OrderedTransactionContext(new VitessEpochProvider(), new VitessRankProvider());
        }
        else {
            transactionContext = new TransactionContext();
        }
        return new VitessOffsetContext(
                connectorConfig, defaultVgtid, clock.currentTimeAsInstant(), transactionContext);
    }

    /**
     * Rotate current and restart vgtid. Only rotate wen necessary.
     */
    public void rotateVgtid(Vgtid newVgtid, Instant commitTime) {
        sourceInfo.rotateVgtid(newVgtid, commitTime);
    }

    public void resetVgtid(Vgtid newVgtid, Instant commitTime) {
        sourceInfo.resetVgtid(newVgtid, commitTime);
    }

    public Vgtid getRestartVgtid() {
        return sourceInfo.getRestartVgtid();
    }

    public void setShard(String shard) {
        sourceInfo.setShard(shard);
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
            result.put(SourceInfo.VGTID_KEY, sourceInfo.getRestartVgtid().toString());
        }
        // put OFFSET_TRANSACTION_ID
        return transactionContext.store(result);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public boolean isSnapshotRunning() {
        return false;
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
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
                + '}';
    }

    public static class Loader implements OffsetContext.Loader<VitessOffsetContext> {

        private final VitessConnectorConfig connectorConfig;

        public Loader(VitessConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public VitessOffsetContext load(Map<String, ?> offset) {
            final String vgtid = (String) offset.get(SourceInfo.VGTID_KEY);
            TransactionContext transactionContext;
            if (connectorConfig.shouldProvideOrderedTransactionMetadata()) {
                transactionContext = OrderedTransactionContext.load(offset, new VitessEpochProvider(), new VitessRankProvider());
            }
            else {
                transactionContext = TransactionContext.load(offset);
            }
            return new VitessOffsetContext(
                    connectorConfig,
                    Vgtid.of(vgtid),
                    null,
                    transactionContext);
        }
    }
}
