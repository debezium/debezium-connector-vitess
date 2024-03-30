/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import java.math.BigInteger;
import java.util.Map;

import io.debezium.connector.vitess.Vgtid;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionInfo;

public class VitessOrderedTransactionContext extends TransactionContext {
    public static final String OFFSET_TRANSACTION_EPOCH = "transaction_epoch";
    public static final String OFFSET_TRANSACTION_RANK = "transaction_rank";
    protected String previousTransactionId = null;
    protected Long transactionEpoch = 0L;
    protected BigInteger transactionRank = null;
    private VitessEpochProvider epochProvider = new VitessEpochProvider();
    private VitessRankProvider rankProvider = new VitessRankProvider();

    public VitessOrderedTransactionContext() {
    }

    public VitessOrderedTransactionContext(TransactionContext transactionContext) {
        super();
        // Copy fields
        this.transactionId = transactionContext.transactionId;
        this.perTableEventCount.putAll(transactionContext.perTableEventCount);
        this.totalEventCount = transactionContext.totalEventCount;
    }

    @Override
    public Map<String, Object> store(Map<String, Object> offset) {
        offset = super.store(offset);
        return epochProvider.store(offset);
    }

    public static VitessOrderedTransactionContext load(Map<String, ?> offsets) {
        TransactionContext transactionContext = TransactionContext.load(offsets);
        VitessOrderedTransactionContext vitessOrderedTransactionContext = new VitessOrderedTransactionContext(transactionContext);
        vitessOrderedTransactionContext.previousTransactionId = (String) offsets.get(TransactionContext.OFFSET_TRANSACTION_ID);
        vitessOrderedTransactionContext.epochProvider.load(offsets);
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
        this.transactionEpoch = this.epochProvider.getEpoch(shard, this.previousTransactionId, vgtid);
        this.transactionRank = this.rankProvider.getRank(Vgtid.of(vgtid).getShardGtid(shard).getGtid());
        this.previousTransactionId = vgtid;
    }
}
