/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_SERVER;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD1;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD2;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARDED_KEYSPACE;
import static io.debezium.connector.vitess.TestHelper.TEST_UNSHARDED_KEYSPACE;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_DISTINCT_HOSTS;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_SHARD1;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_SHARD2;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.OffsetReader;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.connector.vitess.pipeline.txmetadata.ShardEpochMap;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionContext;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionMetadataFactory;
import io.debezium.embedded.KafkaConnectUtil;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

public class VitessConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnectorTest.class);

    @Test
    public void shouldReturnConfigurationDefinition() {
        ConfigDef configDef = new VitessConnector().config();
        assertThat(configDef).isNotNull();
    }

    @Test
    public void shouldReturnVersion() {
        assertThat(new VitessConnector().version()).isNotNull();
    }

    @Test
    public void testTaskConfigsSingle() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
            }
        };
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, null);
        assertThat(taskConfigs.size() == 1);
        assertEquals(taskConfigs.get(0), props);
    }

    @Test
    public void testTaskConfigsNegativeOffsetStorageModeUnset() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
            }
        };
        connector.start(props);
        try {
            connector.taskConfigs(2, null);
            fail("Should not reach here because we don't support multi-tasks when offset.storage.per.task is not set");
        }
        catch (IllegalArgumentException ex) {
            // This is expected();
            LOGGER.info("Expected exception: ", ex);
        }
    }

    @Test
    public void testTaskConfigsNegativeOffsetStorageModeFalse() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "false");
            }
        };
        connector.start(props);
        try {
            connector.taskConfigs(2, null);
            fail("Should not reach here because we don't support multi-tasks when offset.storage.per.task is false");
        }
        catch (IllegalArgumentException ex) {
            // This is expected();
            LOGGER.info("Expected exception: ", ex);
        }
    }

    @Test
    public void testTaskConfigsOffsetStorageModeSingle() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_UNSHARDED_KEYSPACE);
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name(), "0");
                put(VitessConnectorConfig.PREV_NUM_TASKS.name(), "1");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, shards);
        taskConfigs = getConfigWithOffsetsHelper(taskConfigs);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 4);
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG),
                VitessConnector.getTaskKeyName(0, 1, 0));
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG),
                String.join(",", shards));
        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID,
                Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        // assertThat(firstConfig.get(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG)).isNull();
        assertEquals(vgtid.toString(), firstConfig.get(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG));
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsOffsetStorageModeSingleWithOrderMetadata() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_UNSHARDED_KEYSPACE);
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name(), "0");
                put(VitessConnectorConfig.PREV_NUM_TASKS.name(), "1");
                put(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY.name(), VitessOrderedTransactionMetadataFactory.class.getName());
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, shards);
        taskConfigs = getConfigWithOffsetsHelper(taskConfigs);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 4);
        assertThat(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG)).isEqualTo(
                VitessConnector.getTaskKeyName(0, 1, 0));
        assertThat(firstConfig.get(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG)).isEqualTo(
                String.join(",", shards));
        assertThat(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG)).isEqualTo("task0_1_0");

        ShardEpochMap epochMap = new ShardEpochMap(
                Map.of("-4000", 0L, "4000-8000", 0L, "8000-c000", 0L, "c000-", 0L));
        assertThat(firstConfig.get(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG)).isEqualTo(
                epochMap.toString());

        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID,
                Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        assertEquals(vgtid.toString(), firstConfig.get(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG));
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShards() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-01", "01-");
        String shardCsv = String.join(",", shards);
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 3);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID,
                Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        Vgtid vgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(firstConfig)));
        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(0), "current"),
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(1), "current")));
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShardsOrderMetadata() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-01", "01-");
        String shardCsv = String.join(",", shards);
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY.name(), VitessOrderedTransactionMetadataFactory.class.getName());
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 3);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        assertThat(firstConfig.get(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG)).isNull();
        Vgtid vgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(firstConfig)));
        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(0), "current"),
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(1), "current")));
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShardsMultipleTasks() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-80", "80-90", "90-");
        String shardCsv = String.join(",", shards);
        int maxTasks = 2;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(maxTasks));
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        taskConfigs = getConfigWithOffsetsHelper(taskConfigs);
        int expectedConfigSize = 10;
        assertEquals(taskConfigs.size(), maxTasks);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertEquals(firstConfig.size(), expectedConfigSize);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        Vgtid vgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(firstConfig)));
        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(0), "current"),
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(2), "current")));
        Map<String, String> secondConfig = taskConfigs.get(1);
        assertEquals(secondConfig.size(), expectedConfigSize);
        assertEquals(secondConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        Vgtid secondVgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(secondConfig)));
        assertThat(secondVgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(1), "current")));
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testScaleDownTasks() {
        List<String> shards = Arrays.asList(TEST_SHARD1, TEST_SHARD2);
        int prevNumTasks = 2;
        int prevGen = 0;
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, prevGen), getVgtidOffset(VGTID_JSON_SHARD1),
                VitessConnector.getTaskKeyName(1, prevNumTasks, prevGen), getVgtidOffset(VGTID_JSON_SHARD2));

        int numTasks = 1;
        int gen = 1;
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, null, prevVgtids,
                (config) -> config.with(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE));
        String taskKeyName = VitessConnector.getTaskKeyName(0, numTasks, gen);
        Map<String, String> taskOffsets = offsets.get(taskKeyName);
        assertThat(Vgtid.of(taskOffsets.get(SourceInfo.VGTID_KEY))).isEqualTo(Vgtid.of(VGTID_JSON_DISTINCT_HOSTS));
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShardsMultipleTasksOrderMetadata() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-80", "80-90", "90-");
        String shardCsv = String.join(",", shards);
        int maxTasks = 2;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(maxTasks));
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY.name(), VitessOrderedTransactionMetadataFactory.class.getName());
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        taskConfigs = getConfigWithOffsetsHelper(taskConfigs);
        int expectedConfigSize = 12;
        assertEquals(taskConfigs.size(), maxTasks);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size()).isEqualTo(expectedConfigSize);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        VitessConnectorConfig config = new VitessConnectorConfig(Configuration.from(firstConfig));
        Vgtid vgtid = VitessReplicationConnection.defaultVgtid(config);
        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(0), "current"),
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(2), "current")));

        ShardEpochMap expectedShardEpochMap = new ShardEpochMap(Map.of("-80", 0L, "90-", 0L));
        ShardEpochMap actualShardEpochMap = VitessReplicationConnection.defaultShardEpochMap(config);
        assertThat(actualShardEpochMap).isEqualTo(expectedShardEpochMap);

        Map<String, String> secondConfig = taskConfigs.get(1);
        assertEquals(secondConfig.size(), expectedConfigSize);
        assertEquals(secondConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        Vgtid secondVgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(secondConfig)));
        assertThat(secondVgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(1), "current")));
        assertEquals("value", firstConfig.get("key"));

        ShardEpochMap epochMap2 = new ShardEpochMap(
                Map.of("80-90", 0L));
        assertThat(secondConfig.get(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG)).isEqualTo(epochMap2.toString());
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShardsMismatchedGtidsMultipleTasks() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-80", "80-90", "90-");
        String shardCsv = String.join(",", shards);
        String vgtid = VgtidTest.VGTID_JSON;
        int maxTasks = 2;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.VGTID.name(), vgtid);
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(maxTasks));
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.VGTID.name());
        List<String> expectedErrorMessages = List.of("The 'vitess.vgtid' value is invalid: If GTIDs are specified must be specified for all shards");
        assertEquals(configValue.errorMessages(), expectedErrorMessages);
    }

    @Test
    public void testTaskConfigsValidatesDeprecatedConfig() {
        LogInterceptor interceptor = new LogInterceptor(VitessConnectorConfig.class);
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-80", "80-90");
        String shardCsv = String.join(",", shards);
        String vgtid = VgtidTest.VGTID_JSON;
        int maxTasks = 2;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.GTID.name(), vgtid);
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(maxTasks));
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        assertThat(interceptor.containsWarnMessage("Field vitess.gtid is deprecated, use vitess.vgtid instead")).isTrue();
        ConfigValue configValue = results.get(VitessConnectorConfig.GTID.name());
        List<String> expectedErrorMessages = List.of("The 'vitess.gtid' value is invalid: If GTIDs are specified must be specified for matching shards");
        assertEquals(configValue.errorMessages(), expectedErrorMessages);
    }

    @Test
    public void testTaskConfigsMultipleTasksNoShardsMultipleGtids() {
        VitessConnector connector = new VitessConnector();
        String vgtid = VgtidTest.VGTID_JSON;
        int maxTasks = 2;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.VGTID.name(), vgtid);
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(maxTasks));
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.VGTID.name());
        List<String> expectedErrorMessages = List.of("The 'vitess.vgtid' value is invalid: If GTIDs are specified, there must be shards specified");
        assertEquals(configValue.errorMessages(), expectedErrorMessages);
    }

    @Test
    public void testTaskConfigsSingleTaskNoShardsNoGtidsMultipleTasks() {
        VitessConnector connector = new VitessConnector();
        int maxTasks = 2;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(maxTasks));
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.VGTID.name());
        assertTrue(configValue != null && configValue.errorMessages() != null && configValue.errorMessages().size() == 0);
    }

    @Test
    public void testTaskConfigsMultipleTasksMultipleShardsMultipleGtids() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-70", "70-80", "80-90", "90-");
        List<String> gtids = Arrays.asList(TestHelper.TEST_GTID, Vgtid.CURRENT_GTID, TestHelper.TEST_GTID, Vgtid.CURRENT_GTID);
        String shardCsv = String.join(",", shards);
        List<Vgtid.ShardGtid> shardGtids = new ArrayList<>();
        for (int i = 0; i < shards.size(); i++) {
            shardGtids.add(new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(i), gtids.get(i)));
        }
        String gtidString = Vgtid.of(shardGtids).toString();
        int maxTasks = 2;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.VGTID.name(), gtidString);
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(maxTasks));
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        taskConfigs = getConfigWithOffsetsHelper(taskConfigs);
        int expectedConfigSize = 11;
        assertEquals(taskConfigs.size(), maxTasks);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertEquals(firstConfig.size(), expectedConfigSize);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        Vgtid vgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(firstConfig)));
        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(0), TestHelper.TEST_GTID),
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(2), TestHelper.TEST_GTID)));
        Map<String, String> secondConfig = taskConfigs.get(1);
        assertEquals(secondConfig.size(), expectedConfigSize);
        assertEquals(secondConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        Vgtid secondVgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(secondConfig)));
        assertThat(secondVgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(1), Vgtid.CURRENT_GTID),
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(3), Vgtid.CURRENT_GTID)));
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShardsMultipleGtids() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-01", "01-");
        String shardCsv = String.join(",", shards);
        String vgtid = VgtidTest.VGTID_JSON;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.VGTID.name(), vgtid);
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, shards);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 3);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID,
                Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        Vgtid defaultVgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(firstConfig)));
        Vgtid expectedVgtid = Vgtid.of(vgtid);
        assertThat(defaultVgtid).isEqualTo(expectedVgtid);
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsReadGtidWithoutTablePKs() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-80", "80-");
        String shardCsv = String.join(",", shards);
        String vgtid = VgtidTest.VGTID_JSON_NO_PKS;
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.VGTID.name(), vgtid);
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, shards);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 3);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID,
                Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        Vgtid defaultVgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(firstConfig)));
        Vgtid expectedVgtid = Vgtid.of(vgtid);
        assertThat(defaultVgtid).isEqualTo(expectedVgtid);
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShardsMismatchedGtids() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-01", "01-");
        String vgtid = VgtidTest.VGTID_JSON;
        String shardCsv = String.join(",", shards);
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
                put(VitessConnectorConfig.VGTID.name(), vgtid);
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.VGTID.name());
        List<String> expectedErrorMessages = List.of("The 'vitess.vgtid' value is invalid: If GTIDs are specified must be specified for matching shards");
        assertEquals(configValue.errorMessages(), expectedErrorMessages);
    }

    @Test
    public void testTaskConfigsSingleTaskMultipleShardsSnapshotInitial() {
        VitessConnector connector = new VitessConnector();
        List<String> shards = Arrays.asList("-01", "01-");
        String shardCsv = String.join(",", shards);
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_SHARDED_KEYSPACE);
                put(VitessConnectorConfig.SHARD.name(), shardCsv);
            }
        };
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, shards);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 3);
        assertEquals(firstConfig.get(VitessConnectorConfig.SHARD.name()),
                String.join(",", shards));
        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID,
                Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        Vgtid vgtid = VitessReplicationConnection.defaultVgtid(new VitessConnectorConfig(Configuration.from(firstConfig)));
        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(0), ""),
                new Vgtid.ShardGtid(TEST_SHARDED_KEYSPACE, shards.get(1), "")));
        assertEquals("value", firstConfig.get("key"));
    }

    @Test
    public void testTaskConfigsOffsetStorageModeDouble() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_UNSHARDED_KEYSPACE);
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name(), "0");
                put(VitessConnectorConfig.PREV_NUM_TASKS.name(), "1");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2, shards);
        taskConfigs = getConfigWithOffsetsHelper(taskConfigs);
        assertThat(taskConfigs.size() == 2);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 4);
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), "task0_2_0");
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG), "-4000,8000-c000");
        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        List<String> shards0 = Arrays.asList("-4000", "8000-c000");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs);
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG), vgtid0.toString());
        assertEquals(firstConfig.get("key"), "value");
        Map<String, String> secondConfig = taskConfigs.get(1);
        assertThat(secondConfig.size() == 4);
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), "task1_2_0");
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG), "4000-8000,c000-");
        List<String> shards1 = Arrays.asList("4000-8000", "c000-");
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs);
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG), vgtid1.toString());
        assertEquals(secondConfig.get("key"), "value");
    }

    @Test
    public void testTaskConfigsOffsetStorageModeDoubleOrderMetadata() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_UNSHARDED_KEYSPACE);
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name(), "0");
                put(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY.name(), VitessOrderedTransactionMetadataFactory.class.getName());
                put(VitessConnectorConfig.PREV_NUM_TASKS.name(), "1");
                put(VitessConnectorConfig.SNAPSHOT_MODE.name(), VitessConnectorConfig.SnapshotMode.NEVER.getValue());
            }
        };
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2, shards);
        taskConfigs = getConfigWithOffsetsHelper(taskConfigs);
        assertThat(taskConfigs.size() == 2);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 4);
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), "task0_2_0");
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG), "-4000,8000-c000");
        List<String> gtidStrs = Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID);
        List<String> shards0 = Arrays.asList("-4000", "8000-c000");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs);
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG), vgtid0.toString());
        assertEquals(firstConfig.get("key"), "value");

        ShardEpochMap epochMap = new ShardEpochMap(
                Map.of("-4000", 0L, "8000-c000", 0L));
        assertThat(firstConfig.get(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG)).isEqualTo(epochMap.toString());

        Map<String, String> secondConfig = taskConfigs.get(1);
        assertThat(secondConfig.size() == 4);
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), "task1_2_0");
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG), "4000-8000,c000-");
        List<String> shards1 = Arrays.asList("4000-8000", "c000-");
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs);
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG), vgtid1.toString());
        assertEquals(secondConfig.get("key"), "value");

        ShardEpochMap epochMap2 = new ShardEpochMap(
                Map.of("4000-8000", 0L, "c000-", 0L));
        assertThat(secondConfig.get(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG)).isEqualTo(epochMap2.toString());
    }

    @Test
    public void testMultiTaskOnlyAllowedWithOffsetStoragePerTask() {
        Map<String, String> props = new HashMap<>() {
            {
                put("connector.class", "io.debezium.connector.vitess.VitessConnector");
                put("database.hostname", "host1");
                put("database.port", "15999");
                put("database.user", "vitess");
                put("database.password", "vitess-password");
                put("vitess.keyspace", "byuser");
                put("vitess.tablet.type", "MASTER");
                put("database.server.name", "dummy");
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, "2");
                put(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name(), "0");
                put(VitessConnectorConfig.PREV_NUM_TASKS.name(), "1");
            }
        };
        VitessConnector connector = new VitessConnector();
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name());
        assertThat(configValue != null && configValue.errorMessages() != null && configValue.errorMessages().size() == 1);
    }

    @Test
    public void testTaskConfigsNegativeOffsetStorageTaskKeyGen() {
        Map<String, String> props = new HashMap<>() {
            {
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
            }
        };
        VitessConnector connector = new VitessConnector();
        connector.start(props);
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name());
        assertThat(configValue != null && configValue.errorMessages() != null && configValue.errorMessages().size() == 1);
    }

    @Test
    public void testTaskConfigsNegativePrevNumTasks() {
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name(), "0");
            }
        };
        VitessConnector connector = new VitessConnector();
        connector.start(props);
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.PREV_NUM_TASKS.name());
        assertThat(configValue != null && configValue.errorMessages() != null && configValue.errorMessages().size() == 1);
    }

    @Test
    public void testTaskConfigsSameNumTasks() {
        VitessConnector connector = new VitessConnector();
        connector.initialize(new ContextHelper().getSourceConnectorContext());
        Map<String, String> props = new HashMap<>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.KEYSPACE.name(), TEST_UNSHARDED_KEYSPACE);
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
                put(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name(), "2");
                put(VitessConnectorConfig.PREV_NUM_TASKS.name(), "2");
            }
        };
        connector.start(props);
        try {
            List<String> shards = Arrays.asList("s1", "s2");
            List<Map<String, String>> taskProps = connector.taskConfigs(2, null);
            fail("Should not reach here because prev.num.tasks and num.tasks are the same, taskProps:"
                    + taskProps);
        }
        catch (IllegalArgumentException ex) {
            // This is expected();
            LOGGER.info("Expected exception: ", ex);
        }
    }

    @Test
    public void testTaskConfigsOffsetMigrationSingle() {
        List<String> shards = Arrays.asList("s0", "s1");
        List<String> gtidStrs = Arrays.asList("gtid0", "gtid1");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 1;
        try {
            Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, numTasks, getVgtidOffset(vgtid0.toString()), null);
            fail("Should not reach here because prev.num.tasks and num.tasks are the same, vgtids:"
                    + offsets);
        }
        catch (IllegalArgumentException ex) {
            // This is expected();
            LOGGER.info("Expected exception: ", ex);

        }
    }

    @Test
    public void testTaskConfigsOffsetMigrationDouble() {
        List<String> shards = Arrays.asList("s0", "s1");
        List<String> gtidStrs = Arrays.asList("gtid0", "gtid1");
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf("s0", "gtid0", "s1", "gtid1");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 2;
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, 1, getVgtidOffset(vgtid0.toString()), null);
        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr != null);
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                gtidPerShard.put(shardGtid.getShard(), shardGtid.getGtid());
            }
        }
        assertEquals(expectedGtidPerShard, gtidPerShard);
    }

    @Test
    public void testTaskConfigsOffsetMigrationDoubleFromServerToPerTaskOrderMetadata() {
        List<String> shards = Arrays.asList("s0", "s1");
        List<String> gtidStrs = Arrays.asList("gtid0", "gtid1");
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf("s0", "gtid0", "s1", "gtid1");
        ShardEpochMap expectedEpochPerShard = new ShardEpochMap(Collect.hashMapOf("s0", 3L, "s1", 4L));
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 2;

        Map<String, ?> serverOffsets = Map.of(
                SourceInfo.VGTID_KEY, vgtid0.toString(),
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpochPerShard.toString());

        Function<Configuration.Builder, Configuration.Builder> customConfig = (builder) -> builder.with(
                VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class);

        // Store offsets for the server, see if we can read those as our previous and use them
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, 1, serverOffsets, null,
                customConfig);
        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr != null);
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                gtidPerShard.put(shardGtid.getShard(), shardGtid.getGtid());
            }
        }
        assertEquals(expectedGtidPerShard, gtidPerShard);
    }

    @Test
    public void testTaskConfigsOffsetDoubleSubsetValidation() {
        List<String> shards = Arrays.asList("s0", "s1");
        List<String> shardSubset = Arrays.asList("s0");
        List<String> gtidStrs = Arrays.asList("gtid0", "gtid1");

        ShardEpochMap expectedEpochPerShard = new ShardEpochMap(Collect.hashMapOf("s0", 3L, "s1", 4L));

        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 2;

        Map<String, ?> serverOffsets = Map.of(
                SourceInfo.VGTID_KEY, vgtid0.toString(),
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpochPerShard.toString());

        assertThatThrownBy(() -> {
            getOffsetFromStorage(numTasks, shardSubset, gen, 1, serverOffsets, null);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("We will lose gtid positions for some shards if we continue");
    }

    @Test
    public void testTaskConfigsOffsetDoubleSubsetEpochValidation() {
        List<String> shardSubset = Arrays.asList("s0");
        ShardEpochMap expectedEpochPerShard = new ShardEpochMap(Collect.hashMapOf("s0", 3L, "s1", 4L));

        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, Collections.emptyList(), Collections.emptyList());
        final int gen = 1;
        final int numTasks = 2;

        Map<String, ?> serverOffsets = Map.of(
                SourceInfo.VGTID_KEY, vgtid0.toString(),
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpochPerShard.toString());

        Function<Configuration.Builder, Configuration.Builder> customConfig = (builder) -> builder.with(
                VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class);

        assertThatThrownBy(() -> {
            getOffsetFromStorage(numTasks, shardSubset, gen, 1, serverOffsets, null,
                    customConfig);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("We will lose epochs for some shards if we continue");
    }

    @Test
    public void testTaskConfigsOffsetRestartDouble() {
        List<String> shards = Arrays.asList("s0", "s1");
        // Note we are not able to fetch old0/old1 since prevGtids takes precedence over serverVgtid
        List<String> gtidStrs = Arrays.asList("old0", "old1");
        Vgtid serverVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 2;
        final int prevNumTasks = 1;
        List<String> shards0 = List.of("s0");
        List<String> shards1 = List.of("s1");
        List<String> gtidStrs0 = List.of("gtid0");
        List<String> gtidStrs1 = List.of("gtid1");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs0);
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs1);
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), getVgtidOffset(vgtid0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen), getVgtidOffset(vgtid1.toString()));
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, getVgtidOffset(serverVgtid.toString()), prevVgtids);
        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr != null);
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                gtidPerShard.put(shardGtid.getShard(), shardGtid.getGtid());
            }
        }
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf("s0", "gtid0", "s1", "gtid1");
        assertEquals(expectedGtidPerShard, gtidPerShard);
    }

    @Test
    public void testTaskConfigsOffsetRestartDoubleOrderMetadata() {
        List<String> shards = Arrays.asList("s0", "s1");
        // Note we are not able to fetch old0/old1 since prevGtids takes precedence over serverVgtid
        List<String> gtidStrs = Arrays.asList("old0", "old1");
        Vgtid serverVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 2;
        final int prevNumTasks = 1;
        List<String> shards0 = List.of("s0");
        List<String> shards1 = List.of("s1");
        List<String> gtidStrs0 = List.of("gtid0");
        List<String> gtidStrs1 = List.of("gtid1");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs0);
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs1);

        ShardEpochMap expectedEpochPerShard = new ShardEpochMap(Collect.hashMapOf("s0", 3L, "s1", 4L));

        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), getVgtidEpochOffset(vgtid0.toString(), expectedEpochPerShard.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen), getVgtidEpochOffset(vgtid1.toString(), expectedEpochPerShard.toString()));

        Function<Configuration.Builder, Configuration.Builder> customConfig = (builder) -> builder.with(
                VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class);

        Map<String, ?> serverOffsets = Map.of(
                SourceInfo.VGTID_KEY, serverVgtid.toString(),
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, expectedEpochPerShard.toString());

        Map<String, Map<String, String>> offsets = getOffsetFromStorage(
                numTasks, shards, gen, prevNumTasks, serverOffsets, taskOffsets,
                customConfig);

        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr != null);
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                gtidPerShard.put(shardGtid.getShard(), shardGtid.getGtid());
            }

            String epoch = offsets.get(key).get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH);
            assertThat(epoch).isEqualTo(expectedEpochPerShard.toString());
        }
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf("s0", "gtid0", "s1", "gtid1");
        assertEquals(expectedGtidPerShard, gtidPerShard);
    }

    @Test
    public void testTaskConfigsOffsetRestartDoubleIncomplete() {
        List<String> shards = Arrays.asList("s0", "s1");
        List<String> gtidStrs = Arrays.asList("old0", "old1");
        Vgtid serverVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 2;
        final int prevNumTasks = 1;
        List<String> shards0 = List.of("s0");
        List<String> gtidStrs0 = List.of("gtid0");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs0);
        // Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs1);
        // Note that we omit the vgtid1 in prevVgtids so it will fallback to the serverVgtid
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), getVgtidOffset(vgtid0.toString()));
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, getVgtidOffset(serverVgtid.toString()), prevVgtids);
        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr != null);
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                gtidPerShard.put(shardGtid.getShard(), shardGtid.getGtid());
            }
        }
        // Note we got gtid0 from prevGtids, but got old1 from serverGtid
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf("s0", "gtid0", "s1", "old1");
        assertEquals(expectedGtidPerShard, gtidPerShard);
    }

    @Test
    public void testTaskConfigsOffsetRestartDoubleIncompleteOrderMetadata() {
        List<String> shards = Arrays.asList("s0", "s1");
        List<String> gtidStrs = Arrays.asList("old0", "old1");
        Vgtid serverVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards, gtidStrs);
        final int gen = 1;
        final int numTasks = 2;
        final int prevNumTasks = 1;
        List<String> shards0 = List.of("s0");
        List<String> gtidStrs0 = List.of("gtid0");
        Vgtid taskVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs0);
        // Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs1);
        // Note that we omit the vgtid1 in prevVgtids so it will fallback to the serverVgtid

        ShardEpochMap serverEpoch = new ShardEpochMap(Collect.hashMapOf("s0", 3L, "s1", 4L));
        ShardEpochMap taskEpoch = new ShardEpochMap(Collect.hashMapOf("s0", 5L));

        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), getVgtidEpochOffset(taskVgtid.toString(), taskEpoch.toString()));

        Function<Configuration.Builder, Configuration.Builder> customConfig = (builder) -> builder.with(
                VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class);

        Map<String, ?> serverOffsets = Map.of(
                SourceInfo.VGTID_KEY, serverVgtid.toString(),
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, serverEpoch.toString());

        Map<String, Map<String, String>> offsets = getOffsetFromStorage(
                numTasks, shards, gen, prevNumTasks, serverOffsets, taskOffsets,
                customConfig);

        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        Map<String, Long> epochPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr != null);
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            String epoch = offsets.get(key).get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                String shard = shardGtid.getShard();
                gtidPerShard.put(shard, shardGtid.getGtid());
                epochPerShard.put(shard, ShardEpochMap.of(epoch).get(shard));
            }
        }
        // Note we got gtid0 from prevGtids, but got old1 from serverGtid
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf("s0", "gtid0", "s1", "old1");
        Map<String, Long> expectedEpochPerShard = Collect.hashMapOf("s0", 5L, "s1", 4L);
        assertThat(gtidPerShard).isEqualTo(expectedGtidPerShard);
        assertThat(epochPerShard).isEqualTo(expectedEpochPerShard);
    }

    @Test
    public void testTaskConfigsOffsetMigrationQuad() {
        List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf("s0", "gtid0", "s1", "gtid1",
                "s2", "gtid2", "s3", "gtid3");
        List<String> shards0 = Arrays.asList("s0", "s2");
        List<String> shards1 = Arrays.asList("s1", "s3");
        List<String> gtidStrs0 = Arrays.asList("gtid0", "gtid2");
        List<String> gtidStrs1 = Arrays.asList("gtid1", "gtid3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs0);
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs1);
        final int gen = 2;
        final int numTasks = 4;
        final int prevNumTasks = 2;
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen - 1), getVgtidOffset(vgtid0.toString()),
                VitessConnector.getTaskKeyName(1, prevNumTasks, gen - 1), getVgtidOffset(vgtid1.toString()));

        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, null, prevVgtids);
        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr != null);
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                gtidPerShard.put(shardGtid.getShard(), shardGtid.getGtid());
            }
        }
        assertEquals(expectedGtidPerShard, gtidPerShard);
    }

    @Test
    public void testTaskConfigsOffsetMigrationQuadOrderMetadata() {
        List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Map<String, String> expectedGtidPerShard = Collect.hashMapOf(
                "s0", "gtid0", "s1", "gtid1", "s2", "gtid2", "s3", "gtid3");
        Map<String, Long> expectedEpochPerShard = Collect.hashMapOf(
                "s0", 5L, "s1", 4L, "s2", 6L, "s3", 7L);

        List<String> shards0 = Arrays.asList("s0", "s2");
        List<String> shards1 = Arrays.asList("s1", "s3");
        List<String> gtidStrs0 = Arrays.asList("gtid0", "gtid2");
        List<String> gtidStrs1 = Arrays.asList("gtid1", "gtid3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards0, gtidStrs0);
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE, shards1, gtidStrs1);

        ShardEpochMap prevEpoch0 = new ShardEpochMap(Collect.hashMapOf("s0", 5L, "s2", 6L));
        ShardEpochMap prevEpoch1 = new ShardEpochMap(Collect.hashMapOf("s1", 4L, "s3", 7L));

        final int gen = 2;
        final int numTasks = 4;
        final int prevNumTasks = 2;

        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen - 1), getVgtidEpochOffset(vgtid0.toString(), prevEpoch0.toString()),
                VitessConnector.getTaskKeyName(1, prevNumTasks, gen - 1), getVgtidEpochOffset(vgtid1.toString(), prevEpoch1.toString()));

        Function<Configuration.Builder, Configuration.Builder> customConfig = (builder) -> builder.with(
                VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class);

        Map<String, Map<String, String>> offsets = getOffsetFromStorage(
                numTasks, shards, gen, prevNumTasks, null, taskOffsets, customConfig);

        assertThat(offsets.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        Map<String, Long> epochPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = offsets.get(key).get(SourceInfo.VGTID_KEY);
            assertThat(gtidStr).isNotNull();
            Vgtid vgtid = Vgtid.of(gtidStr);
            assertThat(vgtid.getShardGtids().size() == 1);
            String epoch = offsets.get(key).get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH);
            for (int i = 0; i < vgtid.getShardGtids().size(); i++) {
                Vgtid.ShardGtid shardGtid = vgtid.getShardGtids().get(i);
                String shard = shardGtid.getShard();
                gtidPerShard.put(shard, shardGtid.getGtid());
                epochPerShard.put(shard, ShardEpochMap.of(epoch).get(shard));
            }
        }

        assertThat(gtidPerShard).isEqualTo(expectedGtidPerShard);
        assertThat(epochPerShard).isEqualTo(expectedEpochPerShard);
    }

    @Test
    public void testEmptyOffsetStorage() {
        final int numTasks = 2;
        final int gen = 0;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID));

        final Map<String, Map<String, String>> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), getVgtidOffset(vgtid0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen), getVgtidOffset(vgtid1.toString()));
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, -1, null, null);
        Testing.print(String.format("offsets: %s", offsets));
        assertEquals(offsets.size(), 2);
        assertArrayEquals(offsets.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testEmptyOffsetStorageOrderMetadata() {
        final int numTasks = 2;
        final int gen = 0;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");

        // Define Vgtid for each task
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID));

        // Define expected epoch map for each shard (using zero for this test)
        ShardEpochMap expectedEpochMap0 = new ShardEpochMap(Collect.hashMapOf("s0", 0L, "s2", 0L));
        ShardEpochMap expectedEpochMap1 = new ShardEpochMap(Collect.hashMapOf("s1", 0L, "s3", 0L));

        // Create expected offsets with Vgtids and epochs
        final Map<String, Map<String, ?>> expectedOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen),
                getVgtidEpochOffset(vgtid0.toString(), expectedEpochMap0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen),
                getVgtidEpochOffset(vgtid1.toString(), expectedEpochMap1.toString()));

        // Retrieve offsets from storage
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen, -1, null, null,
                builder -> builder.with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class));

        // Debug print for verification
        Testing.print(String.format("offsets: %s", offsets));

        // Assertions
        assertEquals(expectedOffsets.size(), offsets.size());
        for (Map.Entry<String, Map<String, ?>> entry : expectedOffsets.entrySet()) {
            String taskKey = entry.getKey();
            Map<String, ?> expectedValues = entry.getValue();
            Map<String, String> actualValues = offsets.get(taskKey);

            assertThat(expectedValues).isEqualTo(actualValues);
        }
    }

    @Test
    public void testPreviousOffsetStorage() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidOffset(vgtid.toString()));

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));

        final Map<String, Map<String, String>> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidOffset(vgtid0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidOffset(vgtid1.toString()));
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("offsets: %s", offsets));
        assertEquals(offsets.size(), 2);
        assertArrayEquals(offsets.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testPreviousOffsetStorageOrderMetadata() {
        final int gen = 0;
        final int prevNumTasks = 1;

        // Define Vgtid for previous tasks
        Vgtid prevVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        ShardEpochMap prevEpochMap = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s1", 1L, "s2", 1L, "s3", 1L));
        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidEpochOffset(prevVgtid.toString(), prevEpochMap.toString()));

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");

        // Define Vgtids for current tasks
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));

        // Define epoch maps for current tasks
        ShardEpochMap epochMap0 = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s2", 1L));
        ShardEpochMap epochMap1 = new ShardEpochMap(Collect.hashMapOf("s1", 1L, "s3", 1L));

        // Create expected offsets with Vgtids and epochs
        final Map<String, Map<String, ?>> expectedOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1),
                getVgtidEpochOffset(vgtid0.toString(), epochMap0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1),
                getVgtidEpochOffset(vgtid1.toString(), epochMap1.toString()));

        // Retrieve offsets from storage
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, prevNumTasks, null, taskOffsets,
                builder -> builder.with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class));

        // Debug print for verification
        Testing.print(String.format("offsets: %s", offsets));

        // Assertions
        assertEquals(expectedOffsets.size(), offsets.size());
        for (Map.Entry<String, Map<String, ?>> entry : expectedOffsets.entrySet()) {
            String taskKey = entry.getKey();
            Map<String, ?> expectedValues = entry.getValue();
            Map<String, String> actualValues = offsets.get(taskKey);

            assertThat(actualValues).isEqualTo(expectedValues);
        }
    }

    @Test
    public void testExpandingShards() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1"), Arrays.asList("gt0", "gt1"));
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidOffset(vgtid.toString()));

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "current"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "current"));

        // We still expect the code will use the current db shards: s0, s1, s2, s3
        final Map<String, Map<String, String>> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidOffset(vgtid0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidOffset(vgtid1.toString()));
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("offsets: %s", offsets));
        assertEquals(offsets.size(), 2);
        assertArrayEquals(offsets.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testExpandingShardsOrderMetadata() {
        final int gen = 0;
        final int prevNumTasks = 1;

        // Define Vgtid for previous tasks
        Vgtid prevVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1"), Arrays.asList("gt0", "gt1"));
        ShardEpochMap prevEpochMap = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s1", 1L));

        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidEpochOffset(prevVgtid.toString(), prevEpochMap.toString()));

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");

        // Define Vgtids for current tasks
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "current"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "current"));

        // Define epoch maps for current tasks
        ShardEpochMap epochMap0 = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s2", 0L));
        ShardEpochMap epochMap1 = new ShardEpochMap(Collect.hashMapOf("s1", 1L, "s3", 0L));

        // Create expected offsets with Vgtids and epochs
        final Map<String, Map<String, ?>> expectedOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1),
                getVgtidEpochOffset(vgtid0.toString(), epochMap0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1),
                getVgtidEpochOffset(vgtid1.toString(), epochMap1.toString()));

        // Retrieve offsets from storage
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, prevNumTasks, null, taskOffsets,
                builder -> builder.with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class));

        // Debug print for verification
        Testing.print(String.format("offsets: %s", offsets));

        // Assertions
        assertEquals(expectedOffsets.size(), offsets.size());
        for (Map.Entry<String, Map<String, ?>> entry : expectedOffsets.entrySet()) {
            String taskKey = entry.getKey();
            Map<String, ?> expectedValues = entry.getValue();
            Map<String, String> actualValues = offsets.get(taskKey);

            assertThat(actualValues).isEqualTo(expectedValues);
        }
    }

    @Test
    public void testContractingShards() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "current", "current"));
        // We still expect the code will use the current db shards: s0, s1, s2, s3
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidOffset(vgtid0.toString()));

        final int numTasks = 1;
        final List<String> shards = Arrays.asList("s0", "s1");
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "current", "current"));

        try {
            getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
            fail("This call should not reach here.");
        }
        catch (IllegalArgumentException ex) {
            Testing.print(String.format("Got expected exception: {}", ex));
        }
    }

    @Test
    public void testContractingShardsOrderMetadata() {
        final int gen = 0;
        final int prevNumTasks = 1;

        // Define Vgtid for previous tasks
        Vgtid prevVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "current", "current"));
        ShardEpochMap prevEpochMap = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s1", 1L, "s2", 1L, "s3", 1L));
        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidEpochOffset(prevVgtid.toString(), prevEpochMap.toString()));

        final int numTasks = 1;
        final List<String> shards = Arrays.asList("s0", "s1");

        assertThatThrownBy(() -> getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, taskOffsets,
                builder -> builder.with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Previous shards: [s3, s0, s1, s2] is the superset of current shards: [s0, s1].");
    }

    @Test
    public void testCurrentOffsetStorageShardSplit() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidOffset(vgtid.toString()));

        final int numTasks = 2;
        // "s3" split into "s30", "s31"
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s30", "s31");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        // We still expect the code will use the old shards "s3" instead of "s30" and "s31"
        final Map<String, Map<String, ?>> currentGenVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidOffset(vgtid0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidOffset(vgtid1.toString()));
        // Add in current gen maps
        prevVgtids.putAll(currentGenVgtids);

        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("offsets: %s", offsets));
        assertEquals(offsets.size(), 2);
        // We want to assert on vgtid=
        assertArrayEquals(offsets.values().toArray(), currentGenVgtids.values().toArray());
    }

    @Test
    public void testCurrentOffsetStorageShardSplitOrderMetadata() {
        final int gen = 0;
        final int prevNumTasks = 1;

        // Define Vgtid for previous tasks
        Vgtid prevVgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        ShardEpochMap prevEpochMap = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s1", 1L, "s2", 1L, "s3", 1L));
        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidEpochOffset(prevVgtid.toString(), prevEpochMap.toString()));

        final int numTasks = 2;
        // "s3" split into "s30", "s31"
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s30", "s31");

        // Define Vgtids for current tasks
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));

        // Define epoch maps for current tasks
        ShardEpochMap epochMap0 = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s2", 1L));
        ShardEpochMap epochMap1 = new ShardEpochMap(Collect.hashMapOf("s1", 1L, "s3", 1L));

        // Create expected offsets with Vgtids and epochs
        final Map<String, Map<String, ?>> expectedOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1),
                getVgtidEpochOffset(vgtid0.toString(), epochMap0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1),
                getVgtidEpochOffset(vgtid1.toString(), epochMap1.toString()));

        // Retrieve offsets from storage
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, taskOffsets,
                builder -> builder.with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class));

        // Debug print for verification
        Testing.print(String.format("offsets: %s", offsets));

        // Assertions
        assertEquals(expectedOffsets.size(), offsets.size());
        for (Map.Entry<String, Map<String, ?>> entry : expectedOffsets.entrySet()) {
            String taskKey = entry.getKey();
            Map<String, ?> expectedValues = entry.getValue();
            Map<String, String> actualValues = offsets.get(taskKey);

            assertThat(actualValues).isEqualTo(expectedValues);
        }
    }

    @Test
    public void testCurrentOffsetStorageShardSplitIncomplete() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidOffset(vgtid.toString()));

        final int numTasks = 2;
        // "s3" split into "s30", "s31"
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s30", "s31");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        // Put in current gen, but missing one task
        prevVgtids.put(VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidOffset(vgtid1.toString()));

        // We still expect the code will use the old shards "s3" instead of "s30" and "s31"
        final Map<String, Object> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), vgtid1.toString());
        try {
            Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
            fail("This call should not reach here.");
        }
        catch (IllegalArgumentException ex) {
            System.out.println(String.format("Got expected exception: {}", ex));
        }
    }

    @Test
    public void testCurrentOffsetStorageShardSplitIncompleteOrderMetadata() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidOffset(vgtid.toString()));

        final int numTasks = 2;
        // "s3" split into "s30", "s31"
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s30", "s31");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        ShardEpochMap epochMap0 = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s2", 1L));
        ShardEpochMap epochMap1 = new ShardEpochMap(Collect.hashMapOf("s1", 1L, "s3", 1L));

        // Put in current gen, but missing one task
        taskOffsets.put(VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidEpochOffset(vgtid1.toString(), epochMap1.toString()));

        // We still expect the code will use the old shards "s3" instead of "s30" and "s31"
        final Map<String, Object> expectedTaskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidEpochOffset(vgtid0.toString(), epochMap0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidEpochOffset(vgtid1.toString(), epochMap1.toString()));
        assertThatThrownBy(() -> getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, taskOffsets,
                builder -> builder.with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(
                        "No offset found for VitessPartition [sourcePartition={server=test_server, task_key=task0_2_1}]");
    }

    @Test
    public void testCurrentOffsetStorageIncomplete() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Map<String, ?>> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidOffset(vgtid.toString()));

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        // Put in current gen, but missing one task
        prevVgtids.put(VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidOffset(vgtid0.toString()));

        // We still expect the code will use the current db shards: s0, s1, s2, s3
        final Map<String, Map<String, String>> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidOffset(vgtid0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidOffset(vgtid1.toString()));
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("offsets: %s", offsets));
        assertEquals(offsets.size(), 2);
        assertArrayEquals(offsets.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testCurrentOffsetStorageIncompleteOrderMetadata() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        // Define epoch maps for current tasks
        ShardEpochMap epochMap = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s1", 1L, "s2", 1L, "s3", 1L));

        final Map<String, Map<String, ?>> taskOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), getVgtidEpochOffset(vgtid.toString(), epochMap.toString()));

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        ShardEpochMap epochMap0 = new ShardEpochMap(Collect.hashMapOf("s0", 1L, "s2", 1L));
        ShardEpochMap epochMap1 = new ShardEpochMap(Collect.hashMapOf("s1", 1L, "s3", 1L));
        // Put in current gen, but missing one task
        taskOffsets.put(VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidEpochOffset(vgtid0.toString(), epochMap0.toString()));

        // We still expect the code will use the current db shards: s0, s1, s2, s3
        final Map<String, Map<String, ?>> expectedOffsets = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), getVgtidEpochOffset(vgtid0.toString(), epochMap0.toString()),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), getVgtidEpochOffset(vgtid1.toString(), epochMap1.toString()));
        Map<String, Map<String, String>> offsets = getOffsetFromStorage(
                numTasks, shards, gen + 1, 1, null, taskOffsets,
                builder -> builder.with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class));
        Testing.print(String.format("offsets: %s", offsets));
        assertThat(offsets).isEqualTo(expectedOffsets);
    }

    @Test
    public void testHashSameShards() {
        List<String> shardsOne = Arrays.asList("-c0", "c0+");
        List<String> shardsTwo = Arrays.asList("c0+", "-c0");
        assertTrue(VitessConnector.hasSameShards(shardsOne, shardsTwo));

        shardsOne = Arrays.asList("-c0", "c0+", "-c0");
        shardsTwo = Arrays.asList("c0+", "-c0");
        assertTrue(!VitessConnector.hasSameShards(shardsOne, shardsTwo));

        shardsOne = null;
        shardsTwo = Arrays.asList("c0+", "-c0");
        assertTrue(!VitessConnector.hasSameShards(shardsOne, shardsTwo));
    }

    @Test
    public void testTableIncludeList() {
        String keyspace = "ks";
        List<String> allTables = Arrays.asList("t1", "t22", "t3");
        String tableIncludeList = new String("ks.t1,ks.t2.*");
        Configuration config = Configuration.from(Map.of(
                VitessConnectorConfig.TABLE_INCLUDE_LIST.name(), tableIncludeList,
                VitessConnectorConfig.KEYSPACE.name(), keyspace));
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        List<String> includedTables = VitessConnector.getIncludedTables(connectorConfig, allTables);
        List<String> expectedTables = Arrays.asList("t1", "t22");
        assertEquals(expectedTables, includedTables);
    }

    @Test
    public void testTableIncludeListShouldExcludeTablesWithSuffix() {
        String keyspace = "ks";
        List<String> allTables = Arrays.asList("t1", "t2", "t22", "t13");
        String tableIncludeList = new String("ks.t1,ks.t2");
        Configuration config = Configuration.from(Map.of(
                VitessConnectorConfig.TABLE_INCLUDE_LIST.name(), tableIncludeList,
                VitessConnectorConfig.KEYSPACE.name(), keyspace));
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(config);
        List<String> includedTables = VitessConnector.getIncludedTables(connectorConfig, allTables);
        List<String> expectedTables = Arrays.asList("t1", "t2");
        assertEquals(expectedTables, includedTables);
    }

    private boolean isEmptyOffsets(Map<String, ?> offsets) {
        return offsets == null || offsets.isEmpty();
    }

    private void storeOffsets(OffsetBackingStore offsetStore, Map<String, ?> serverOffsets, Map<String, Map<String, ?>> taskOffsets) {
        if (isEmptyOffsets(serverOffsets) && isEmptyOffsets(taskOffsets)) {
            Testing.print("Empty gtids to store to offset.");
            return;
        }
        OffsetStorageWriter offsetWriter = getWriter(offsetStore);

        if (!isEmptyOffsets(serverOffsets)) {
            Testing.print(String.format("Server offsets: %s", serverOffsets));
            Map<String, Object> sourcePartition = Collect.hashMapOf(
                    VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER);
            offsetWriter.offset(sourcePartition, serverOffsets);
        }

        if (!isEmptyOffsets(taskOffsets)) {
            Testing.print(String.format("Task offsets: %s", taskOffsets));
            for (String task : taskOffsets.keySet()) {
                Map<String, Object> sourcePartition = Collect.hashMapOf(
                        VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER,
                        VitessPartition.TASK_KEY_PARTITION_KEY, task);
                Map<String, ?> offset = taskOffsets.get(task);
                offsetWriter.offset(sourcePartition, offset);
            }
        }

        offsetWriter.beginFlush();
        Future<Void> f = offsetWriter.doFlush(null);
        try {
            f.get(100, TimeUnit.MILLISECONDS);
        }
        catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    private static OffsetStorageWriter getWriter(OffsetBackingStore offsetStore) {
        final String engineName = "testOffset";
        final Converter keyConverter = new JsonConverter();
        Map<String, Object> converterConfig = Collect.hashMapOf("schemas.enable", false);
        keyConverter.configure(converterConfig, true);
        final Converter valueConverter = new JsonConverter();
        valueConverter.configure(converterConfig, false);
        OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, engineName,
                keyConverter, valueConverter);
        return offsetWriter;
    }

    private static List<Map<String, String>> getConfigWithOffsetsHelper(List<Map<String, String>> initialTaskConfigs) {
        List<Map<String, String>> taskConfigs = new ArrayList();
        for (Map<String, String> config : initialTaskConfigs) {
            VitessConnectorTask task = new VitessConnectorTask();
            task.initialize(new VitessConnectorTaskTest.ContextHelper().getSourceTaskContext());
            Configuration newConfig = task.getConfigWithOffsets(Configuration.from(config));
            taskConfigs.add(newConfig.asMap());
        }
        return taskConfigs;
    }

    private Map<String, Map<String, String>> getTaskOffsets(OffsetBackingStore offsetStore, int numTasks, List<String> shards,
                                                            int gen, int prevNumTasks, Function<Configuration.Builder, Configuration.Builder> customConfig) {
        final Configuration config = customConfig.apply(
                TestHelper.defaultConfig(
                        false,
                        true,
                        numTasks,
                        gen,
                        prevNumTasks,
                        null,
                        VitessConnectorConfig.SnapshotMode.NEVER))
                .build();
        final String engineName = "testOffset";
        final Converter keyConverter = new JsonConverter();
        Map<String, Object> converterConfig = Collect.hashMapOf("schemas.enable", false);
        keyConverter.configure(converterConfig, true);
        final Converter valueConverter = new JsonConverter();
        valueConverter.configure(converterConfig, false);
        final OffsetStorageReaderImpl offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName,
                keyConverter, valueConverter);

        VitessConnector connector = new VitessConnector();
        SourceConnectorContext connectorContext = new SourceConnectorContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return offsetReader;
            }

            @Override
            public void requestTaskReconfiguration() {
            }

            @Override
            public void raiseError(Exception e) {
                LOGGER.error("Unexpected exception", e);
                fail(e.getMessage());
            }
        };
        connector.initialize(connectorContext);
        connector.start(config.asMap());

        SourceTaskContext sourceTaskContext = new SourceTaskContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return offsetReader;
            }

            public Map<String, String> configs() {
                return config.asMap();
            }
        };

        List<Map<String, String>> taskConfigs = connector.taskConfigs(numTasks, shards);
        Map<String, Map<String, String>> offsetsMap = new HashMap<>();
        for (Map<String, String> taskConfig : taskConfigs) {
            Map<String, String> taskOffsetMap = new HashMap();
            VitessConnectorTask task = new VitessConnectorTask();
            task.initialize(sourceTaskContext);
            final VitessConnectorConfig connectorConfig = new VitessConnectorConfig(
                    task.getConfigWithOffsets(Configuration.from(taskConfig)));
            Set<VitessPartition> partitions = new VitessPartition.Provider(connectorConfig).getPartitions();
            OffsetReader<VitessPartition, VitessOffsetContext, OffsetContext.Loader<VitessOffsetContext>> reader = new OffsetReader<>(
                    sourceTaskContext.offsetStorageReader(), new VitessOffsetContext.Loader(
                            connectorConfig));
            Map<VitessPartition, VitessOffsetContext> offsets = reader.offsets(partitions);
            Offsets<VitessPartition, VitessOffsetContext> previousOffsets = Offsets.of(offsets);

            final VitessOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();
            Vgtid vgtid;
            if (previousOffset == null) {
                vgtid = VitessReplicationConnection.defaultVgtid(connectorConfig);
            }
            else {
                vgtid = previousOffset.getRestartVgtid();
            }
            taskOffsetMap.put(SourceInfo.VGTID_KEY, vgtid.toString());
            if (VitessOffsetRetriever.isShardEpochMapEnabled(connectorConfig) && previousOffset == null) {
                ShardEpochMap shardEpochMap = VitessReplicationConnection.defaultShardEpochMap(connectorConfig);
                taskOffsetMap.put(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, shardEpochMap.toString());
            }
            else if (VitessOffsetRetriever.isShardEpochMapEnabled(connectorConfig)) {
                ShardEpochMap shardEpochMap = ShardEpochMap.of((String) previousOffset.getOffset().get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH));
                taskOffsetMap.put(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, shardEpochMap.toString());
            }
            offsetsMap.put(taskConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), taskOffsetMap);
        }
        connector.stop();
        offsetReader.close();
        return offsetsMap;
    }

    private Map<String, Map<String, String>> getOffsetFromStorage(int numTasks, List<String> shards, int gen, int prevNumTasks,
                                                                  Map<String, ?> serverOffsets, Map<String, Map<String, ?>> taskOffsets) {
        return getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, serverOffsets, taskOffsets, Function.identity());
    }

    private Map<String, Map<String, String>> getOffsetFromStorage(int numTasks, List<String> shards, int gen, int prevNumTasks,
                                                                  Map<String, ?> serverOffsets, Map<String, Map<String, ?>> taskOffsets,
                                                                  Function<Configuration.Builder, Configuration.Builder> customConfig) {
        final OffsetBackingStore offsetStore = KafkaConnectUtil.memoryOffsetBackingStore();
        offsetStore.start();

        storeOffsets(offsetStore, serverOffsets, taskOffsets);
        Map<String, Map<String, String>> offsets = getTaskOffsets(offsetStore, numTasks, shards, gen, prevNumTasks, customConfig);

        offsetStore.stop();
        return offsets;
    }

    private Map<String, String> getVgtidOffset(String vgtid) {
        return Map.of(SourceInfo.VGTID_KEY, vgtid);
    }

    private Map<String, ?> getVgtidEpochOffset(String vgtid, String epoch) {
        return Map.of(SourceInfo.VGTID_KEY, vgtid, VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, epoch);
    }

    static class ContextHelper {
        OffsetBackingStore offsetStore;
        String engineName = "testOffset";
        SourceConnectorContext sourceConnectorContext;

        ContextHelper() {
            this.offsetStore = KafkaConnectUtil.memoryOffsetBackingStore();
            this.sourceConnectorContext = initSourceConnectorContext();
        }

        public SourceConnectorContext getSourceConnectorContext() {
            return this.sourceConnectorContext;
        }

        private SourceConnectorContext initSourceConnectorContext() {
            offsetStore.start();

            final Converter keyConverter = new JsonConverter();
            Map<String, Object> converterConfig = Collect.hashMapOf("schemas.enable", false);
            keyConverter.configure(converterConfig, true);
            final Converter valueConverter = new JsonConverter();
            valueConverter.configure(converterConfig, false);
            final OffsetStorageReaderImpl offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName,
                    keyConverter, valueConverter);

            SourceConnectorContext sourceConnectorContext = new SourceConnectorContext() {
                @Override
                public void requestTaskReconfiguration() {
                }

                @Override
                public void raiseError(Exception e) {
                }

                @Override
                public OffsetStorageReader offsetStorageReader() {
                    return offsetReader;
                }

            };
            return sourceConnectorContext;
        }
    }
}
