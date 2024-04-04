/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_TEMPLATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VgtidTest;

public class VitessEpochProviderTest {

    private String prefix = "MySQL56/";
    private String host1Tx1 = "027c67a2-c0b0-11ec-8a34-0ed0087913a5:1-11418261";
    private String host1Tx2 = "027c67a2-c0b0-11ec-8a34-0ed0087913a5:1-11418262";
    private String host2Tx1 = "08fb1cf3-0ce5-11ed-b921-0a8939501751:1-1443715";

    private String previousTxId = prefix + String.join(",", host1Tx1, host2Tx1);
    private String txId = prefix + String.join(",", host1Tx2, host2Tx1);
    private String txIdShrunk = prefix + String.join(",", host1Tx2);

    private String txIdVersion5 = "MySQL57/" + String.join(",", host1Tx2);
    private String txIdVersion8 = "MySQL82/" + String.join(",", host1Tx2);

    @Test
    public void testGetEpochSameHostSet() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, txId);
        assertThat(epoch).isEqualTo(0);
    }

    @Test
    public void testGetEpochVgtid() {
        VitessEpochProvider provider = new VitessEpochProvider();
        String expectedEpoch = "{\"-80\": 5}";
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpoch));
        Long epoch = provider.getEpoch("-80", VgtidTest.VGTID_JSON, VgtidTest.VGTID_JSON);
        assertThat(epoch).isEqualTo(5);
    }

    @Test
    public void snapshotIncrementsEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider();
        String vgtidJsonEmpty = String.format(
                VGTID_JSON_TEMPLATE,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.EMPTY_GTID,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.EMPTY_GTID);
        Long epoch = provider.getEpoch(VgtidTest.TEST_SHARD, vgtidJsonEmpty, VgtidTest.VGTID_JSON);
        assertThat(epoch).isEqualTo(1L);
    }

    @Test
    public void fastForwardVgtidIncrementsEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider();
        String vgtidJsonCurrent = String.format(
                VGTID_JSON_TEMPLATE,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.EMPTY_GTID,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.EMPTY_GTID);
        Long epoch = provider.getEpoch(VgtidTest.TEST_SHARD, vgtidJsonCurrent, VgtidTest.VGTID_JSON);
        assertThat(epoch).isEqualTo(1L);
    }

    @Test
    public void testInvalidCurrentGtid() {
        Long expectedEpoch = 0L;
        VitessEpochProvider provider = new VitessEpochProvider();
        Long epoch = provider.getEpoch("-80", VgtidTest.VGTID_JSON, VgtidTest.VGTID_JSON);
        assertThat(epoch).isEqualTo(expectedEpoch);
        String vgtidJsonCurrent = String.format(
                VGTID_JSON_TEMPLATE,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.EMPTY_GTID,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.EMPTY_GTID);
        assertThatThrownBy(() -> {
            provider.getEpoch("-80", VgtidTest.VGTID_JSON, vgtidJsonCurrent);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Invalid");
    }

    @Test
    public void testInvalidEmptyGtid() {
        Long expectedEpoch = 0L;
        VitessEpochProvider provider = new VitessEpochProvider();
        Long epoch = provider.getEpoch("-80", VgtidTest.VGTID_JSON, VgtidTest.VGTID_JSON);
        assertThat(epoch).isEqualTo(expectedEpoch);
        String vgtidJsonEmpty = String.format(
                VGTID_JSON_TEMPLATE,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.EMPTY_GTID,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.EMPTY_GTID);
        assertThatThrownBy(() -> {
            provider.getEpoch("-80", VgtidTest.VGTID_JSON, vgtidJsonEmpty);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Invalid");
    }

    @Test
    public void testGetEpochShrunkHostSet() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, txIdShrunk);
        assertThat(epoch).isEqualTo(1);
    }

    @Test
    public void testGetEpochExpandHostSet() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, txId);
        assertThat(epoch).isEqualTo(0);
    }

    @Test
    public void testGetEpochDisjointThrowsException() {
        Assertions.assertThatThrownBy(() -> {
            VitessEpochProvider.getEpochForGtid(0L, previousTxId, "foo:1-2,bar:2-4");
        }).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testVersionUpgradeDoesNotAffectEpoch() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, txIdVersion5, txIdVersion8);
        assertThat(epoch).isEqualTo(0L);
    }
}
