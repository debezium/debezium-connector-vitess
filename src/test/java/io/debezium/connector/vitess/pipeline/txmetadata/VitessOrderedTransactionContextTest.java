/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import io.debezium.connector.vitess.SourceInfo;

public class VitessOrderedTransactionContextTest {

    private static final Schema sourceStructSchema = SchemaBuilder.struct().field(SourceInfo.VGTID_KEY, Schema.STRING_SCHEMA);

    @Test
    public void shouldInit() {
        new VitessOrderedTransactionContext();
    }

    @Test
    public void shouldLoad() {
        String expectedId = null;
        String expectedEpoch = "{\"-80\": 0}";
        Map offsets = Map.of(
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpoch);
        VitessOrderedTransactionContext metadata = new VitessOrderedTransactionContext();
        metadata.load(offsets);
        assertThat(metadata.previousTransactionId).isEqualTo(expectedId);
    }

    @Test
    public void shouldLoadWithNull() {
        String expectedId = null;
        Long expectedEpoch = 0L;
        Map offsets = Collections.emptyMap();
        VitessOrderedTransactionContext metadata = new VitessOrderedTransactionContext();
        metadata.load(offsets);
        assertThat(metadata.previousTransactionId).isEqualTo(expectedId);
        assertThat(metadata.transactionEpoch).isEqualTo(expectedEpoch);
    }

    @Test
    public void shouldUpdateEpoch() {
        VitessOrderedTransactionContext metadata = new VitessOrderedTransactionContext();

        String expectedTxId = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3,host2:3-4\", \"shard\": \"-80\"}]";
        BigInteger expectedRank = new BigInteger("7");
        long expectedEpoch = 0;
        String expectedShard = "-80";

        VitessTransactionInfo transactionInfo = new VitessTransactionInfo(expectedTxId, expectedShard);
        metadata.beginTransaction(transactionInfo);
        assertThat(metadata.transactionRank).isEqualTo(expectedRank);
        assertThat(metadata.transactionEpoch).isEqualTo(expectedEpoch);

        String expectedTxId2 = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3\", \"shard\": \"-80\"}]";
        BigInteger expectedRank2 = new BigInteger("3");
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
        assertThat(metadata.transactionRank).isEqualTo(7);

        String expectedTxId2 = "[{\"keyspace\": \"foo\", \"gtid\": \"host1:1-3\", \"shard\": \"-80\"}]";
        VitessTransactionInfo transactionInfo2 = new VitessTransactionInfo(expectedTxId2, expectedShard);
        metadata.beginTransaction(transactionInfo2);
        assertThat(metadata.transactionRank).isEqualTo(3);
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
