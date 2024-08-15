/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static io.debezium.connector.vitess.TestHelper.TEST_SHARD1;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD1_EPOCH;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD_TO_EPOCH;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_TEMPLATE;
import static io.debezium.connector.vitess.VgtidTest.VGTID_BOTH_CURRENT;
import static io.debezium.connector.vitess.VgtidTest.VGTID_JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.vitess.TablePrimaryKeysTest;
import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VgtidTest;
import io.debezium.connector.vitess.VitessConnectorConfig;

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

    private List<String> shards = List.of(VgtidTest.TEST_SHARD, VgtidTest.TEST_SHARD2);

    String vgtidJsonCurrent = String.format(
            VGTID_JSON_TEMPLATE,
            VgtidTest.TEST_KEYSPACE,
            VgtidTest.TEST_SHARD,
            Vgtid.CURRENT_GTID,
            VgtidTest.TEST_KEYSPACE,
            VgtidTest.TEST_SHARD2,
            Vgtid.CURRENT_GTID);

    @Test
    public void testGetEpochSameHostSet() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, txId, false);
        assertThat(epoch).isEqualTo(0);
    }

    @Test
    public void testLoadsEpochFromOffsets() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, TEST_SHARD_TO_EPOCH.toString()));
        Long epoch = provider.getEpoch(TEST_SHARD1, VGTID_JSON, VGTID_JSON);
        assertThat(epoch).isEqualTo(TEST_SHARD1_EPOCH);
    }

    @Test
    public void testInitializeConfigEpochWithOffsetStorage() {
        Configuration config = Configuration.create()
                .with(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG, TestHelper.TEST_SHARD_TO_EPOCH.toString())
                .with(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK, "true")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        VitessEpochProvider provider = VitessEpochProvider.initialize(connectorConfig);
        Long epoch = provider.getEpoch(TestHelper.TEST_SHARD1, VGTID_JSON, VGTID_JSON);
        assertThat(epoch).isEqualTo(TEST_SHARD1_EPOCH);
    }

    @Test
    public void testInitializeConfigEpochWithShardList() {
        Configuration config = Configuration.create()
                .with(VitessConnectorConfig.SHARD, TestHelper.TEST_SHARD1)
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        VitessEpochProvider provider = VitessEpochProvider.initialize(connectorConfig);
        assertThat(provider.getShardEpochMap().get(TEST_SHARD1)).isEqualTo(0);
    }

    @Test
    public void snapshotIncrementsEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        String vgtidJsonEmpty = String.format(
                VGTID_JSON_TEMPLATE,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.EMPTY_GTID,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.EMPTY_GTID);
        Long epoch = provider.getEpoch(VgtidTest.TEST_SHARD, vgtidJsonEmpty, VGTID_JSON);
        assertThat(epoch).isEqualTo(1L);
    }

    @Test
    public void fastForwardVgtidIncrementsEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        String vgtidJsonCurrent = String.format(
                VGTID_JSON_TEMPLATE,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.EMPTY_GTID,
                VgtidTest.TEST_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.EMPTY_GTID);
        Long epoch = provider.getEpoch(VgtidTest.TEST_SHARD, vgtidJsonCurrent, VGTID_JSON);
        assertThat(epoch).isEqualTo(1L);
    }

    @Test
    public void currentVgtidIncrementsEpochForAllShards() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        Long epochShard1 = provider.getEpoch(VgtidTest.TEST_SHARD, vgtidJsonCurrent, VGTID_JSON);
        Long epochShard2 = provider.getEpoch(VgtidTest.TEST_SHARD2, VGTID_JSON, VGTID_JSON);
        assertThat(epochShard1).isEqualTo(1L);
        assertThat(epochShard2).isEqualTo(1L);
    }

    @Test
    public void splitShard() {
        VitessEpochProvider provider = new VitessEpochProvider();
        String singleShardVgtidTemplate = "[" +
                "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\",\"table_p_ks\":%s}" +
                "]";
        String vgtid1 = String.format(
                singleShardVgtidTemplate,
                TestHelper.TEST_SHARDED_KEYSPACE,
                TestHelper.TEST_SHARD,
                txId,
                TablePrimaryKeysTest.TEST_LAST_PKS_JSON);
        String vgtid2 = String.format(
                VGTID_JSON_TEMPLATE,
                TestHelper.TEST_SHARDED_KEYSPACE,
                TestHelper.TEST_SHARD1,
                txId,
                TestHelper.TEST_SHARDED_KEYSPACE,
                TestHelper.TEST_SHARD2,
                txId);
        Long epochShard1 = provider.getEpoch(TestHelper.TEST_SHARD, VGTID_BOTH_CURRENT, vgtid1);
        Long epochShard2 = provider.getEpoch(TestHelper.TEST_SHARD1, vgtid1, vgtid2);
        assertThat(epochShard1).isEqualTo(0L);
        assertThat(epochShard2).isEqualTo(0L);
    }

    @Test
    public void nullPreviousVgtidWithStoredEpochShouldThrowException() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        int expectedEpoch = 1;
        String shardToEpoch = String.format("{\"%s\": %d}", TEST_SHARD1, expectedEpoch);
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, shardToEpoch));
        assertThatThrownBy(() -> {
            provider.getEpoch(VgtidTest.TEST_SHARD, null, VGTID_JSON);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Previous vgtid string cannot be null");
    }

    @Test
    public void missingEpochWithPreviousVgtidShouldThrowException() {
        VitessEpochProvider provider = new VitessEpochProvider();
        int expectedEpoch = 1;
        String shardToEpoch = String.format("{\"%s\": %d}", TEST_SHARD1, expectedEpoch);
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, shardToEpoch));
        assertThatThrownBy(() -> {
            provider.getEpoch(VgtidTest.TEST_SHARD, VGTID_JSON, VGTID_JSON);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Previous epoch cannot be null");
    }

    @Test
    public void matchingGtidReturnsInitialEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        int expectedEpoch = 0;
        Long epoch = provider.getEpoch(VgtidTest.TEST_SHARD, VGTID_JSON, VGTID_JSON);
        assertThat(epoch).isEqualTo(expectedEpoch);
    }

    @Test
    public void testInvalidCurrentGtid() {
        Long expectedEpoch = 0L;
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        Long epoch = provider.getEpoch("-80", VGTID_JSON, VGTID_JSON);
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
            provider.getEpoch("-80", VGTID_JSON, vgtidJsonCurrent);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Invalid");
    }

    @Test
    public void testInvalidEmptyGtid() {
        Long expectedEpoch = 0L;
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards));
        Long epoch = provider.getEpoch("-80", VGTID_JSON, VGTID_JSON);
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
            provider.getEpoch("-80", VGTID_JSON, vgtidJsonEmpty);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Invalid");
    }

    @Test
    public void testGetEpochShrunkHostSet() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, txIdShrunk, false);
        assertThat(epoch).isEqualTo(1);
    }

    @Test
    public void testGetEpochExpandHostSet() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, txId, false);
        assertThat(epoch).isEqualTo(0);
    }

    @Test
    public void testGetEpochDisjointThrowsException() {
        Assertions.assertThatThrownBy(() -> {
            VitessEpochProvider.getEpochForGtid(0L, previousTxId, "foo:1-2,bar:2-4", false);
        }).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testVersionUpgradeDoesNotAffectEpoch() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, txIdVersion5, txIdVersion8, false);
        assertThat(epoch).isEqualTo(0L);
    }
}
