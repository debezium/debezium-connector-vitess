/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class OrderedTransactionContextTest {

    @Test
    public void shouldInit() {
        EpochProvider epochProvider = new VitessEpochProvider();
        RankProvider rankProvider = new VitessRankProvider();
        OrderedTransactionContext context = new OrderedTransactionContext(epochProvider, rankProvider);
        context.getTransactionId();
    }

    @Test
    public void shouldLoad() {
        String expectedId = "foo";
        Long expectedEpoch = 1L;
        String expectedRank = "10";
        Map offsets = Map.of(
                OrderedTransactionContext.OFFSET_TRANSACTION_ID, expectedId,
                OrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpoch,
                OrderedTransactionContext.OFFSET_TRANSACTION_RANK, expectedRank);
        EpochProvider epochProvider = new VitessEpochProvider();
        RankProvider rankProvider = new VitessRankProvider();
        OrderedTransactionContext context = OrderedTransactionContext.load(offsets, epochProvider, rankProvider);
        assertThat(context.getTransactionId()).isEqualTo(expectedId);
        assertThat(context.getTransactionEpoch()).isEqualTo(expectedEpoch);
        assertThat(context.getTransactionRank()).isEqualTo(new BigInteger(expectedRank));
    }

    @Test
    public void shouldLoadWithNull() {
        String expectedId = null;
        Long expectedEpoch = null;
        BigInteger expectedRank = null;
        Map offsets = Collections.emptyMap();
        EpochProvider epochProvider = new VitessEpochProvider();
        RankProvider rankProvider = new VitessRankProvider();
        OrderedTransactionContext context = OrderedTransactionContext.load(offsets, epochProvider, rankProvider);
        assertThat(context.getTransactionId()).isEqualTo(expectedId);
        assertThat(context.getTransactionEpoch()).isEqualTo(expectedEpoch);
        assertThat(context.getTransactionRank()).isEqualTo(expectedRank);
    }

    @Test
    public void shouldUpdateEpoch() {
        EpochProvider epochProvider = new VitessEpochProvider();
        RankProvider rankProvider = new VitessRankProvider();
        OrderedTransactionContext context = new OrderedTransactionContext(epochProvider, rankProvider);

        String expectedTxId = ("host1:1-3,host2:3-4");
        context.beginTransaction(expectedTxId);
        assertThat(context.getTransactionId()).isEqualTo(expectedTxId);
        assertThat(context.getTransactionEpoch()).isEqualTo(0);

        String expectedTxId2 = "host1:1-3";
        context.beginTransaction(expectedTxId2);
        assertThat(context.getTransactionId()).isEqualTo(expectedTxId2);
        assertThat(context.getTransactionEpoch()).isEqualTo(1);
    }

    @Test
    public void shouldUpdateRank() {
        EpochProvider epochProvider = new VitessEpochProvider();
        RankProvider rankProvider = new VitessRankProvider();
        OrderedTransactionContext context = new OrderedTransactionContext(epochProvider, rankProvider);

        String expectedTxId = ("host1:1-3,host2:3-4");
        context.beginTransaction(expectedTxId);
        assertThat(context.getTransactionRank()).isEqualTo(7);

        String expectedTxId2 = "host1:1-3";
        context.beginTransaction(expectedTxId2);
        assertThat(context.getTransactionRank()).isEqualTo(3);
    }

    @Test
    public void shouldStoreOffsets() {
        EpochProvider epochProvider = new VitessEpochProvider();
        RankProvider rankProvider = new VitessRankProvider();
        OrderedTransactionContext context = new OrderedTransactionContext(epochProvider, rankProvider);

        String expectedTxId = ("host1:1-3,host2:3-4");
        context.beginTransaction(expectedTxId);

        Map offsets = new HashMap();
        Map actualOffsets = context.store(offsets);
        assertThat(actualOffsets.get(OrderedTransactionContext.OFFSET_TRANSACTION_ID)).isEqualTo(expectedTxId);
        assertThat(actualOffsets.get(OrderedTransactionContext.OFFSET_TRANSACTION_EPOCH)).isEqualTo(0L);
        assertThat(actualOffsets.get(OrderedTransactionContext.OFFSET_TRANSACTION_RANK)).isEqualTo("7");
    }

    @Test
    public void shouldKnowTransactionInProgress() {
        EpochProvider epochProvider = new VitessEpochProvider();
        RankProvider rankProvider = new VitessRankProvider();
        OrderedTransactionContext context = new OrderedTransactionContext(epochProvider, rankProvider);

        String expectedTxId = ("host1:1-3,host2:3-4");
        context.beginTransaction(expectedTxId);
        assertThat(context.isTransactionInProgress()).isTrue();
        context.endTransaction();
        assertThat(context.isTransactionInProgress()).isFalse();
    }

}
