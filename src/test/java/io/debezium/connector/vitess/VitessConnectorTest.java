/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_SERVER;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARDED_KEYSPACE;
import static io.debezium.connector.vitess.TestHelper.TEST_UNSHARDED_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, shards);
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
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        int expectedConfigSize = 9;
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
    public void testTaskConfigsSingleTaskNoShardsMultipleGtidsMultipleTasks() {
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
    public void testTaskConfigsSingleTaskMultipleShardsMultipleGtidsMultipleTasks() {
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
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        int expectedConfigSize = 10;
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
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2, shards);
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
            Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen, numTasks, vgtid0.toString(), null);
            fail("Should not reach here because prev.num.tasks and num.tasks are the same, vgtids:"
                    + vgtids);
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
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen, 1, vgtid0.toString(), null);
        assertThat(vgtids.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = vgtids.get(key);
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
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen), vgtid1.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, serverVgtid.toString(), prevVgtids);
        assertThat(vgtids.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = vgtids.get(key);
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
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), vgtid0.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, serverVgtid.toString(), prevVgtids);
        assertThat(vgtids.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = vgtids.get(key);
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
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen - 1), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, prevNumTasks, gen - 1), vgtid1.toString());

        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen, prevNumTasks, null, prevVgtids);
        assertThat(vgtids.size() == numTasks);
        Map<String, String> gtidPerShard = new HashMap<>();
        for (int tid = 0; tid < numTasks; tid++) {
            String key = VitessConnector.getTaskKeyName(tid, numTasks, gen);
            String gtidStr = vgtids.get(key);
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
    public void testEmptyOffsetStorage() {
        final int numTasks = 2;
        final int gen = 0;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList(Vgtid.CURRENT_GTID, Vgtid.CURRENT_GTID));

        final Map<String, String> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen), vgtid1.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen, -1, null, null);
        Testing.print(String.format("vgtids: %s", vgtids));
        assertEquals(vgtids.size(), 2);
        assertArrayEquals(vgtids.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testPreviousOffsetStorage() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), vgtid.toString());

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));

        final Map<String, Object> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), vgtid1.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("vgtids: %s", vgtids));
        assertEquals(vgtids.size(), 2);
        assertArrayEquals(vgtids.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testExpandingShards() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1"), Arrays.asList("gt0", "gt1"));
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), vgtid.toString());

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "current"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "current"));

        // We still expect the code will use the current db shards: s0, s1, s2, s3
        final Map<String, Object> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), vgtid1.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("vgtids: %s", vgtids));
        assertEquals(vgtids.size(), 2);
        assertArrayEquals(vgtids.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testContractingShards() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "current", "current"));
        // We still expect the code will use the current db shards: s0, s1, s2, s3
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), vgtid0.toString());

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
    public void testCurrentOffsetStorageShardSplit() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), vgtid.toString());

        final int numTasks = 2;
        // "s3" split into "s30", "s31"
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s30", "s31");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        // We still expect the code will use the old shards "s3" instead of "s30" and "s31"
        final Map<String, Object> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), vgtid1.toString());
        // Add in current gen maps
        prevVgtids.putAll(expectedVgtids);

        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("vgtids: %s", vgtids));
        assertEquals(vgtids.size(), 2);
        assertArrayEquals(vgtids.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testCurrentOffsetStorageShardSplitIncomplete() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), vgtid.toString());

        final int numTasks = 2;
        // "s3" split into "s30", "s31"
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s30", "s31");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        // Put in current gen, but missing one task
        prevVgtids.put(VitessConnector.getTaskKeyName(1, numTasks, gen + 1), vgtid1.toString());

        // We still expect the code will use the old shards "s3" instead of "s30" and "s31"
        final Map<String, Object> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), vgtid1.toString());
        try {
            Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
            fail("This call should not reach here.");
        }
        catch (IllegalArgumentException ex) {
            Testing.print(String.format("Got expected exception: {}", ex));
        }
    }

    @Test
    public void testCurrentOffsetStorageIncomplete() {
        final int gen = 0;
        final int prevNumTasks = 1;
        Vgtid vgtid = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s1", "s2", "s3"), Arrays.asList("gt0", "gt1", "gt2", "gt3"));
        final Map<String, Object> prevVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, prevNumTasks, gen), vgtid.toString());

        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));
        // Put in current gen, but missing one task
        prevVgtids.put(VitessConnector.getTaskKeyName(0, numTasks, gen + 1), vgtid0.toString());

        // We still expect the code will use the current db shards: s0, s1, s2, s3
        final Map<String, Object> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks, gen + 1), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks, gen + 1), vgtid1.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, gen + 1, 1, null, prevVgtids);
        Testing.print(String.format("vgtids: %s", vgtids));
        assertEquals(vgtids.size(), 2);
        assertArrayEquals(vgtids.values().toArray(), expectedVgtids.values().toArray());
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
        List<String> includedTables = VitessConnector.getIncludedTables(keyspace, tableIncludeList, allTables);
        List<String> expectedTables = Arrays.asList("t1", "t22");
        assertEquals(expectedTables, includedTables);
    }

    private void storeOffsets(OffsetBackingStore offsetStore, String serverVgtid, Map<String, Object> prevVgtids) {
        if (serverVgtid == null && (prevVgtids == null || prevVgtids.isEmpty())) {
            Testing.print("Empty gtids to store to offset.");
            return;
        }
        final String engineName = "testOffset";
        final Converter keyConverter = new JsonConverter();
        Map<String, Object> converterConfig = Collect.hashMapOf("schemas.enable", false);
        keyConverter.configure(converterConfig, true);
        final Converter valueConverter = new JsonConverter();
        valueConverter.configure(converterConfig, false);
        OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, engineName,
                keyConverter, valueConverter);
        if (serverVgtid != null) {
            Testing.print(String.format("Server vgtids: %s", serverVgtid));
            Map<String, Object> sourcePartition = Collect.hashMapOf(
                    VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER);
            Map<String, Object> offset = Collect.hashMapOf(SourceInfo.VGTID_KEY, serverVgtid);
            offsetWriter.offset(sourcePartition, offset);
        }
        if (prevVgtids != null) {
            Testing.print(String.format("Previous vgtids: %s", prevVgtids));
            for (String key : prevVgtids.keySet()) {
                Map<String, Object> sourcePartition = Collect.hashMapOf(
                        VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER,
                        VitessPartition.TASK_KEY_PARTITION_KEY, key);
                Map<String, Object> offset = Collect.hashMapOf(SourceInfo.VGTID_KEY, prevVgtids.get(key));
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

    private Map<String, String> getTaskOffsets(OffsetBackingStore offsetStore, int numTasks, List<String> shards,
                                               int gen, int prevNumTasks) {
        final Configuration config = TestHelper.defaultConfig(false, true, numTasks, gen, prevNumTasks, null, VitessConnectorConfig.SnapshotMode.NEVER).build();
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
        Map<String, String> vgtids = new HashMap<>();
        for (Map<String, String> taskConfig : taskConfigs) {
            VitessConnectorTask task = new VitessConnectorTask();
            task.initialize(sourceTaskContext);

            final VitessConnectorConfig connectorConfig = new VitessConnectorConfig(Configuration.from(taskConfig));
            Set<VitessPartition> partitions = new VitessPartition.Provider(connectorConfig).getPartitions();
            OffsetReader<VitessPartition, VitessOffsetContext, OffsetContext.Loader<VitessOffsetContext>> reader = new OffsetReader<>(
                    sourceTaskContext.offsetStorageReader(), new VitessOffsetContext.Loader(
                            connectorConfig));
            Map<VitessPartition, VitessOffsetContext> offsets = reader.offsets(partitions);
            Offsets<VitessPartition, VitessOffsetContext> previousOffsets = Offsets.of(offsets);

            final VitessOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();
            Vgtid vgtid = previousOffset == null ? VitessReplicationConnection.defaultVgtid(connectorConfig)
                    : previousOffset.getRestartVgtid();
            vgtids.put(taskConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), vgtid.toString());
        }
        connector.stop();
        offsetReader.close();
        return vgtids;
    }

    private Map<String, String> getOffsetFromStorage(int numTasks, List<String> shards, int gen, int prevNumTasks,
                                                     String serverVgtid, Map<String, Object> prevVgtids) {
        final OffsetBackingStore offsetStore = KafkaConnectUtil.memoryOffsetBackingStore();
        offsetStore.start();

        storeOffsets(offsetStore, serverVgtid, prevVgtids);
        Map<String, String> vgtids = getTaskOffsets(offsetStore, numTasks, shards, gen, prevNumTasks);

        offsetStore.stop();
        return vgtids;
    }
}
