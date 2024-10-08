/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import java.math.BigDecimal;
import java.util.Map;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.vitess.SourceInfo;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionInfo;

public class VitessOrderedTransactionContext extends TransactionContext {
    public static final String OFFSET_TRANSACTION_EPOCH = "transaction_epoch";
    public static final String OFFSET_TRANSACTION_RANK = "transaction_rank";
    protected String previousVgtid = null;
    protected Long transactionEpoch = 0L;
    protected BigDecimal transactionRank = null;
    private VitessEpochProvider epochProvider = new VitessEpochProvider();
    private VitessConnectorConfig config = null;

    public VitessOrderedTransactionContext() {
    }

    public VitessOrderedTransactionContext(TransactionContext transactionContext) {
        super();
        // Copy fields
        super.setTransactionId(transactionContext.getTransactionId());
        super.putPerTableEventCount(transactionContext.getPerTableEventCount());
        super.setTotalEventCount(transactionContext.getTotalEventCount());
    }

    /**
     * Stores the needed information for determining Vitess rank & Epoch. Example (excluding standard fields added by super class):
     * Input Offset map:
     * {
     *     "transaction_id": "[{\"keyspace\":\ks1\",\"shard\":\"-80\",\"gtid\":\"MySQL56/host1:123,host2:234\",\"table_p_ks\":[]} \
     *                         {\"keyspace\":\ks1\",\"shard\":\"80-\",\"gtid\":\"MySQL56/host1:123,host2:234\",\"table_p_ks\":[]}"
     * }
     * Current shard to epoch map, in epoch provider:
     * {
     *     "-80": 0,
     *     "80-", 1
     * }
     * Output offset map:
     * {
     *     "transaction_id": "[{\"keyspace\":\ks1\",\"shard\":\"-80\",\"gtid\":\"MySQL56/host1:123,host2:234\",\"table_p_ks\":[]} \
     *                         {\"keyspace\":\ks1\",\"shard\":\"80-\",\"gtid\":\"MySQL56/host1:123,host2:234\",\"table_p_ks\":[]}"
     *     "transaction_epoch": {
     *          "-80": 0,
     *          "80-", 1
     *     }
     * }
     *
     * Note: there is no need to store the transaction rank. We get the previous transaction ID from the "transaction_id" field
     * and use that to compute the epoch. Rank requires no state (sum of max offsets of all hosts).
     *
     * @param offset
     * @return
     */
    @Override
    public Map<String, Object> store(Map<String, Object> offset) {
        offset = super.store(offset);
        return epochProvider.store(offset);
    }

    /**
     * Called when there are previous offsets, and we need to create a {@link VitessOrderedTransactionContext}
     * from those offsets.
     * @param offsets Offsets to load
     * @return
     */
    @Override
    public TransactionContext newTransactionContextFromOffsets(Map<String, ?> offsets) {
        return VitessOrderedTransactionContext.load(offsets, this.config);
    }

    public static VitessOrderedTransactionContext load(Map<String, ?> offsets, VitessConnectorConfig config) {
        TransactionContext transactionContext = TransactionContext.load(offsets);
        VitessOrderedTransactionContext vitessOrderedTransactionContext = new VitessOrderedTransactionContext(transactionContext);
        vitessOrderedTransactionContext.previousVgtid = (String) offsets.get(SourceInfo.VGTID_KEY);
        vitessOrderedTransactionContext.epochProvider.load(offsets, config);
        return vitessOrderedTransactionContext;
    }

    /**
     * Always called when we need to create a TransactionContext. Returns a {@link VitessOrderedTransactionContext} initialized
     * based on the {@link VitessConnectorConfig}. If offsets exist, then it will be followed by a call to
     * {@link VitessOrderedTransactionContext#newTransactionContextFromOffsets(Map)}
     * from those offsets.
     * @param config
     * @return {@link VitessOrderedTransactionContext}
     */
    public static VitessOrderedTransactionContext initialize(VitessConnectorConfig config) {
        VitessOrderedTransactionContext vitessOrderedTransactionContext = new VitessOrderedTransactionContext();
        vitessOrderedTransactionContext.epochProvider = VitessEpochProvider.initialize(config);
        vitessOrderedTransactionContext.previousVgtid = VitessReplicationConnection.defaultVgtid(config).toString();
        vitessOrderedTransactionContext.config = config;
        return vitessOrderedTransactionContext;
    }

    @Override
    public void beginTransaction(TransactionInfo transactionInfo) {
        super.beginTransaction(transactionInfo);
        VitessTransactionInfo vitessTransactionInfo = (VitessTransactionInfo) transactionInfo;
        beginTransaction(vitessTransactionInfo.getShard(), vitessTransactionInfo.getTransactionId());
    }

    @Override
    public void endTransaction() {
        super.endTransaction();
        this.transactionEpoch = null;
        this.transactionRank = null;
    }

    private void beginTransaction(String shard, String vgtid) {
        this.transactionEpoch = this.epochProvider.getEpoch(shard, this.previousVgtid, vgtid);
        this.transactionRank = VitessRankProvider.getRank(Vgtid.of(vgtid).getShardGtid(shard).getGtid());
        this.previousVgtid = vgtid;
    }

    @Override
    public String toString() {
        return "VitessOrderedTransactionContext [currentTransactionId=" + getTransactionId() + ", perTableEventCount="
                + getPerTableEventCount() + ", totalEventCount=" + getTotalEventCount() + "]" + ", previousVgtid=" + previousVgtid
                + ", transactionEpoch=" + transactionEpoch + ", transactionRank=" + transactionRank;
    }

    public String getPreviousVgtid() {
        return previousVgtid;
    }

    public Long getTransactionEpoch() {
        return transactionEpoch;
    }

    public BigDecimal getTransactionRank() {
        return transactionRank;
    }

    @VisibleForTesting
    public VitessEpochProvider getEpochProvider() {
        return epochProvider;
    }

}
