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
import static io.debezium.connector.vitess.TestHelper.VGTID_SINGLE_SHARD_JSON_TEMPLATE;
import static io.debezium.connector.vitess.VgtidTest.TEST_SHARD2;
import static io.debezium.connector.vitess.VgtidTest.VGTID_BOTH_CURRENT;
import static io.debezium.connector.vitess.VgtidTest.VGTID_JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

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
    private String errorOnCurrentOverrideValue = "Current GTID cannot be override value if previous is standard";

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
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, txId);
        assertThat(epoch).isEqualTo(0);
    }

    @Test
    public void testLoadsEpochFromOffsets() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
        VitessConnectorConfig config = new VitessConnectorConfig(Configuration.empty());
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, TEST_SHARD_TO_EPOCH.toString()), config);
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
    public void testInitializeConfigEpochWithInheritEpoch() {
        Configuration config = Configuration.create()
                .with(VitessConnectorConfig.SHARD, TestHelper.TEST_SHARD1)
                .with(VitessConnectorConfig.INHERIT_EPOCH, true)
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        VitessEpochProvider provider = VitessEpochProvider.initialize(connectorConfig);
        assertThat(provider.isInheritEpochEnabled()).isEqualTo(true);
    }

    @Test
    public void snapshotIncrementsEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
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
        // Both shards had their epoch incremented
        assertThat(provider.getShardEpochMap().getMap()).isEqualTo(Map.of(TEST_SHARD1, 1L, TEST_SHARD2, 1L));
    }

    @Test
    public void fastForwardVgtidIncrementsEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
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
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
        Long epochShard1 = provider.getEpoch(VgtidTest.TEST_SHARD, vgtidJsonCurrent, VGTID_JSON);
        assertThat(provider.getShardEpochMap()).isEqualTo(new ShardEpochMap(Map.of(VgtidTest.TEST_SHARD, 1L, VgtidTest.TEST_SHARD2, 1L)));
        Long epochShard2 = provider.getEpoch(VgtidTest.TEST_SHARD2, VGTID_JSON, VGTID_JSON);
        assertThat(provider.getShardEpochMap()).isEqualTo(new ShardEpochMap(Map.of(VgtidTest.TEST_SHARD, 1L, VgtidTest.TEST_SHARD2, 1L)));
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
    public void splitShardInheritsEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider(new ShardEpochMap(Map.of("0", 0L)), true);
        String vgtidSingleCurrent = String.format(
                VGTID_SINGLE_SHARD_JSON_TEMPLATE,
                TestHelper.TEST_SHARDED_KEYSPACE,
                TestHelper.TEST_SHARD,
                Vgtid.CURRENT_GTID);
        String vgtid1 = String.format(
                VGTID_SINGLE_SHARD_JSON_TEMPLATE,
                TestHelper.TEST_SHARDED_KEYSPACE,
                TestHelper.TEST_SHARD,
                txId);
        String vgtid2 = String.format(
                VGTID_JSON_TEMPLATE,
                TestHelper.TEST_SHARDED_KEYSPACE,
                TestHelper.TEST_SHARD1,
                txId,
                TestHelper.TEST_SHARDED_KEYSPACE,
                TestHelper.TEST_SHARD2,
                txId);
        Long epochShard1 = provider.getEpoch(TestHelper.TEST_SHARD, vgtidSingleCurrent, vgtid1);
        assertThat(epochShard1).isEqualTo(1L);
        Long epochShard2 = provider.getEpoch(TestHelper.TEST_SHARD1, vgtid1, vgtid2);
        assertThat(epochShard2).isEqualTo(2L);
    }

    @Test
    public void nullPreviousVgtidWithStoredEpochShouldThrowException() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
        int expectedEpoch = 1;
        String shardToEpoch = String.format("{\"%s\": %d}", TEST_SHARD1, expectedEpoch);
        VitessConnectorConfig config = new VitessConnectorConfig(Configuration.empty());
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, shardToEpoch), config);
        assertThatThrownBy(() -> {
            provider.getEpoch(VgtidTest.TEST_SHARD, null, VGTID_JSON);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Previous vgtid string cannot be null");
    }

    @Test
    public void missingEpochWithPreviousVgtidShouldThrowException() {
        VitessEpochProvider provider = new VitessEpochProvider();
        int expectedEpoch = 1;
        String shardToEpoch = String.format("{\"%s\": %d}", TEST_SHARD1, expectedEpoch);
        VitessConnectorConfig config = new VitessConnectorConfig(Configuration.empty());
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, shardToEpoch), config);
        assertThatThrownBy(() -> {
            provider.getEpoch(VgtidTest.TEST_SHARD, VGTID_JSON, VGTID_JSON);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining("Previous epoch cannot be null");
    }

    @Test
    public void testGtidPartialCurrent() {
        VitessEpochProvider provider = new VitessEpochProvider();
        VitessConnectorConfig config = new VitessConnectorConfig(Configuration.empty());
        provider.load(Map.of(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH,
                new ShardEpochMap(Map.of("f0-f8", 1L, "30-38", 1L, "b0-b8", 1L, "70-78", 1L)).toString()), config);
        String shard = "f0-f8";
        String vgtidAllCurrent = "[" +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"30-38\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"70-78\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"b0-b8\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"f0-f8\",\"gtid\":\"current\",\"table_p_ks\":[]}]";
        String vgtidOneShardWithGtid = "[" +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"30-38\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"70-78\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"b0-b8\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"f0-f8\",\"gtid\":\"MySQL56/host3:1-144525090\",\"table_p_ks\":[]}]";
        String vgtidOneShardCurrent = "[" +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"30-38\",\"gtid\":\"MySQL56/host1:1-450588997\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"70-78\",\"gtid\":\"MySQL56/host2:1-368122129\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"b0-b8\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"f0-f8\",\"gtid\":\"MySQL56/host3:1-144525093\",\"table_p_ks\":[]}]";
        String vgtidOneShardCurrentNewGtid = "[" +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"30-38\",\"gtid\":\"MySQL56/host1:1-450588998\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"70-78\",\"gtid\":\"MySQL56/host2:1-368122129\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"b0-b8\",\"gtid\":\"current\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"f0-f8\",\"gtid\":\"MySQL56/host3:1-144525093\",\"table_p_ks\":[]}]";
        String vgtidNoShardCurrent = "[" +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"30-38\",\"gtid\":\"MySQL56/host1:1-450588997\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"70-78\",\"gtid\":\"MySQL56/host2:1-368122129\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"b0-b8\",\"gtid\":\"host4:1-3\",\"table_p_ks\":[]}," +
                "{\"keyspace\":\"keyspace1\",\"shard\":\"f0-f8\",\"gtid\":\"MySQL56/host3:1-144525093\",\"table_p_ks\":[]}]";
        // The first transaction will have at least one shard with an actual GTID
        provider.getEpoch(shard, vgtidAllCurrent, vgtidOneShardWithGtid);
        // Eventually all but one shard will have a GTID
        provider.getEpoch(shard, vgtidOneShardWithGtid, vgtidOneShardCurrent);
        // We have received a legit GTID for all shards except one, that one can still be current
        provider.getEpoch(shard, vgtidOneShardCurrent, vgtidOneShardCurrentNewGtid);
        // We can continue to receive current for that GTID indefinitely
        provider.getEpoch(shard, vgtidOneShardCurrentNewGtid, vgtidOneShardCurrentNewGtid);
        // Eventually, we receive a GTID for that shard
        provider.getEpoch("b0-b8", vgtidOneShardCurrentNewGtid, vgtidNoShardCurrent);
        // After that if we received current again that would be an error
        assertThatThrownBy(() -> {
            provider.getEpoch("b0-b8", vgtidNoShardCurrent, vgtidOneShardCurrent);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining(errorOnCurrentOverrideValue);
        // Assert that if we received current again even for a non-transaction shard, that would be an error
        assertThatThrownBy(() -> {
            provider.getEpoch(shard, vgtidNoShardCurrent, vgtidOneShardCurrent);
        }).isInstanceOf(DebeziumException.class).hasMessageContaining(errorOnCurrentOverrideValue);
    }

    @Test
    public void matchingGtidReturnsInitialEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
        int expectedEpoch = 0;
        Long epoch = provider.getEpoch(VgtidTest.TEST_SHARD, VGTID_JSON, VGTID_JSON);
        assertThat(epoch).isEqualTo(expectedEpoch);
    }

    @Test
    public void testInvalidCurrentGtid() {
        Long expectedEpoch = 0L;
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
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
        }).isInstanceOf(DebeziumException.class).hasMessageContaining(errorOnCurrentOverrideValue);
    }

    @Test
    public void testInvalidEmptyGtid() {
        Long expectedEpoch = 0L;
        VitessEpochProvider provider = new VitessEpochProvider(ShardEpochMap.init(shards), false);
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
        }).isInstanceOf(DebeziumException.class).hasMessageContaining(errorOnCurrentOverrideValue);
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
    public void testGetEpochDisjointIncrementsEpoch() {
        long previousEpoch = 0L;
        long epoch = VitessEpochProvider.getEpochForGtid(0L, previousTxId, "foo:1-2,bar:2-4");
        assertThat(epoch).isEqualTo(previousEpoch + 1);
    }

    @Test
    public void testVersionUpgradeDoesNotAffectEpoch() {
        Long epoch = VitessEpochProvider.getEpochForGtid(0L, txIdVersion5, txIdVersion8);
        assertThat(epoch).isEqualTo(0L);
    }
}
