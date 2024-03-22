/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

public class OrderedTransactionContext extends TransactionContext {

    protected static final String OFFSET_TRANSACTION_ID = "transaction_id";
    protected static final String OFFSET_TRANSACTION_EPOCH = "transaction_epoch";
    protected static final String OFFSET_TRANSACTION_RANK = "transaction_rank";

    private static final String OFFSET_TABLE_COUNT_PREFIX = "transaction_data_collection_order_";
    private static final int OFFSET_TABLE_COUNT_PREFIX_LENGTH = OFFSET_TABLE_COUNT_PREFIX.length();
    private String transactionId = null;
    private final Map<String, Long> perTableEventCount = new HashMap();
    private final Map<String, Long> viewPerTableEventCount;
    private long totalEventCount;
    private String previousTransactionId = null;
    private Long transactionEpoch;
    private BigInteger transactionRank;

    private EpochProvider epochProvider;
    private RankProvider rankProvider;

    public OrderedTransactionContext(EpochProvider epochProvider, RankProvider rankProvider) {
        this.epochProvider = epochProvider;
        this.rankProvider = rankProvider;
        this.viewPerTableEventCount = Collections.unmodifiableMap(this.perTableEventCount);
        this.totalEventCount = 0L;
        this.transactionEpoch = 0L;
        this.transactionRank = null;
    }

    private void reset() {
        this.transactionId = null;
        this.totalEventCount = 0L;
        this.perTableEventCount.clear();
        this.transactionRank = null;
    }

    @Override
    public Map<String, Object> store(Map<String, Object> offset) {
        if (!Objects.isNull(this.transactionId)) {
            offset.put(OFFSET_TRANSACTION_ID, this.transactionId);
        }
        if (!Objects.isNull(this.transactionEpoch)) {
            offset.put(OFFSET_TRANSACTION_EPOCH, this.transactionEpoch);
        }
        if (!Objects.isNull(this.transactionRank)) {
            offset.put(OFFSET_TRANSACTION_RANK, this.transactionRank.toString());
        }

        Iterator var3 = this.perTableEventCount.entrySet().iterator();

        while (var3.hasNext()) {
            Map.Entry<String, Long> e = (Map.Entry) var3.next();
            offset.put(OFFSET_TABLE_COUNT_PREFIX + e.getKey(), e.getValue());
        }
        return offset;
    }

    public static OrderedTransactionContext load(Map<String, ?> offsets, EpochProvider epochProvider, RankProvider rankProvider) {
        OrderedTransactionContext context = new OrderedTransactionContext(epochProvider, rankProvider);
        context.transactionId = (String) offsets.get(OFFSET_TRANSACTION_ID);
        context.previousTransactionId = (String) offsets.get(OFFSET_TRANSACTION_ID);

        context.transactionEpoch = (Long) offsets.get(OFFSET_TRANSACTION_EPOCH);
        String transactionRankString = (String) offsets.get(OFFSET_TRANSACTION_RANK);
        if (transactionRankString == null) {
            context.transactionRank = null;
        }
        else {
            context.transactionRank = new BigInteger(transactionRankString);
        }

        Iterator var3 = offsets.entrySet().iterator();

        while (var3.hasNext()) {
            Map.Entry<String, Object> offset = (Map.Entry) var3.next();
            if ((offset.getKey()).startsWith(OFFSET_TABLE_COUNT_PREFIX)) {
                String dataCollectionId = (offset.getKey()).substring(OFFSET_TABLE_COUNT_PREFIX_LENGTH);
                Long count = (Long) offset.getValue();
                context.perTableEventCount.put(dataCollectionId, count);
            }
        }

        context.totalEventCount = context.perTableEventCount.values().stream().mapToLong((x) -> x).sum();
        return context;
    }

    @Override
    public boolean isTransactionInProgress() {
        return !Objects.isNull(this.transactionId);
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public long getTotalEventCount() {
        return this.totalEventCount;
    }

    @Override
    public void beginTransaction(String txId) {
        this.previousTransactionId = this.transactionId;
        this.reset();
        this.transactionId = txId;
        transactionEpoch = this.epochProvider.getEpoch(this.transactionEpoch, previousTransactionId, txId);
        transactionRank = this.rankProvider.getRank(txId);
    }

    @Override
    public void endTransaction() {
        this.reset();
    }

    @Override
    public long event(DataCollectionId source) {
        ++this.totalEventCount;
        String sourceName = source.toString();
        long dataCollectionEventOrder = (Long) this.perTableEventCount.getOrDefault(sourceName, 0L) + 1L;
        this.perTableEventCount.put(sourceName, dataCollectionEventOrder);
        return dataCollectionEventOrder;
    }

    @Override
    public Map<String, Long> getPerTableEventCount() {
        return this.viewPerTableEventCount;
    }

    @Override
    public String toString() {
        return "TransactionContext [" +
                "currentTransactionId=" + this.transactionId +
                ", perTableEventCount=" + this.perTableEventCount +
                ", totalEventCount=" + this.totalEventCount +
                ", transactionEpoch=" + this.transactionEpoch +
                ", transactionRank=" + this.transactionRank +
                "]";
    }

    public Long getTransactionEpoch() {
        return transactionEpoch;
    }

    public BigInteger getTransactionRank() {
        return transactionRank;
    }
}
