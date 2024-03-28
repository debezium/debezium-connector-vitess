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
import io.debezium.pipeline.txmetadata.TransactionOrderMetadata;

public class VitessTransactionOrderMetadata implements TransactionOrderMetadata {

    public static final String OFFSET_TRANSACTION_EPOCH = "transaction_epoch";
    public static final String OFFSET_TRANSACTION_RANK = "transaction_rank";
    protected String previousTransactionId = null;
    protected Long transactionEpoch;
    protected BigInteger transactionRank;
    private VitessEpochProvider epochProvider = new VitessEpochProvider();
    private VitessRankProvider rankProvider = new VitessRankProvider();

    @Override
    public Map<String, Object> store(Map<String, Object> offset) {
        return epochProvider.store(offset);
    }

    @Override
    public void load(Map<String, ?> offsets) {
        this.previousTransactionId = (String) offsets.get(TransactionContext.OFFSET_TRANSACTION_ID);
        epochProvider.load(offsets);
    }

    @Override
    public void beginTransaction(TransactionInfo transactionInfo) {
        VitessTransactionInfo vitessTransactionInfo = (VitessTransactionInfo) transactionInfo;
        beginTransaction(vitessTransactionInfo.getShard(), vitessTransactionInfo.getTransactionId());
    }

    @Override
    public void endTransaction() {
        this.transactionEpoch = null;
        this.transactionRank = null;
    }

    private void beginTransaction(String shard, String vgtid) {
        this.transactionEpoch = this.epochProvider.getEpoch(shard, this.previousTransactionId, vgtid);
        this.transactionRank = this.rankProvider.getRank(Vgtid.of(vgtid).getShardGtid(shard).getGtid());
        this.previousTransactionId = vgtid;
    }
}
