/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import io.debezium.connector.vitess.SourceInfo;
import io.debezium.connector.vitess.VgtidTest;
import io.debezium.pipeline.txmetadata.TransactionContext;

public class VitessOrderedTransactionContextTest {

    private static final Schema sourceStructSchema = SchemaBuilder.struct().field(SourceInfo.VGTID_KEY, Schema.STRING_SCHEMA);

    @Test
    public void shouldInit() {
        new VitessOrderedTransactionContext();
    }

    @Test
    public void shouldLoad() {
        String expectedId = VgtidTest.VGTID_JSON;
        String expectedEpoch = "{\"-80\": 5}";
        Map offsets = Map.of(
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpoch,
                TransactionContext.OFFSET_TRANSACTION_ID, expectedId);
        VitessOrderedTransactionContext context = VitessOrderedTransactionContext.load(offsets);
        assertThat(context.previousVgtid).isEqualTo(expectedId);
        context.beginTransaction(new VitessTransactionInfo(VgtidTest.VGTID_JSON, "-80"));
        assertThat(context.transactionEpoch).isEqualTo(5);
    }

    @Test
    public void shouldLoadWithNull() {
        String expectedId = null;
        Long expectedEpoch = 0L;
        Map offsets = Collections.emptyMap();
        VitessOrderedTransactionContext metadata = new VitessOrderedTransactionContext();
        metadata.load(offsets);
        assertThat(metadata.previousVgtid).isEqualTo(expectedId);
        assertThat(metadata.transactionEpoch).isEqualTo(expectedEpoch);
    }

    @Test
    public void shouldUpdateEpoch() {
        VitessOrderedTransactionContext metadata = new VitessOrderedTransactionContext();

        String expectedTxId = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3,host2:3-4\", \"shard\": \"-80\"}]";
        BigDecimal expectedRank = new BigDecimal("7");
        long expectedEpoch = 0;
        String expectedShard = "-80";

        VitessTransactionInfo transactionInfo = new VitessTransactionInfo(expectedTxId, expectedShard);
        metadata.beginTransaction(transactionInfo);
        assertThat(metadata.transactionRank).isEqualTo(expectedRank);
        assertThat(metadata.transactionEpoch).isEqualTo(expectedEpoch);

        String expectedTxId2 = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3\", \"shard\": \"-80\"}]";
        BigDecimal expectedRank2 = new BigDecimal("3");
        long expectedEpoch2 = 1;

        VitessTransactionInfo transactionInfo2 = new VitessTransactionInfo(expectedTxId2, expectedShard);
        metadata.beginTransaction(transactionInfo2);
        assertThat(metadata.transactionRank).isEqualTo(expectedRank2);
        assertThat(metadata.transactionEpoch).isEqualTo(expectedEpoch2);
    }

    @Test
    public void shouldUpdateRank() {
        VitessOrderedTransactionContext metadata = new VitessOrderedTransactionContext();

        String expectedTxId = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3,host2:3-4\", \"shard\": \"-80\"}]";
        String expectedShard = "-80";

        VitessTransactionInfo transactionInfo = new VitessTransactionInfo(expectedTxId, expectedShard);
        metadata.beginTransaction(transactionInfo);
        assertThat(metadata.transactionRank).isEqualTo(new BigDecimal(7));

        String expectedTxId2 = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3\", \"shard\": \"-80\"}]";
        VitessTransactionInfo transactionInfo2 = new VitessTransactionInfo(expectedTxId2, expectedShard);
        metadata.beginTransaction(transactionInfo2);
        assertThat(metadata.transactionRank).isEqualTo(new BigDecimal(3));
    }

    @Test
    public void shouldStoreOffsets() {
        VitessOrderedTransactionContext metadata = new VitessOrderedTransactionContext();

        String expectedTxId = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3,host2:3-4\", \"shard\": \"-80\"}]";
        String expectedShard = "-80";

        VitessTransactionInfo transactionInfo = new VitessTransactionInfo(expectedTxId, expectedShard);
        metadata.beginTransaction(transactionInfo);

        Map offsets = new HashMap();
        String expectedEpoch = "{\"-80\":0}";
        Map actualOffsets = metadata.store(offsets);
        assertThat(actualOffsets.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH)).isEqualTo(expectedEpoch);
    }
}
