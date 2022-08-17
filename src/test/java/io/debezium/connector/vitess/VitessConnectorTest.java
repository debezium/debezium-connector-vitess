/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_SERVER;
import static io.debezium.connector.vitess.TestHelper.TEST_UNSHARDED_KEYSPACE;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
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
        Map<String, String> props = new HashMap<String, String>() {
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
        Map<String, String> props = new HashMap<String, String>() {
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
            LOGGER.info("Expected exception: {}", ex);
        }
    }

    @Test
    public void testTaskConfigsNegativeOffsetStorageModeFalse() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<String, String>() {
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
            LOGGER.info("Expected exception: {}", ex);
        }
    }

    @Test
    public void testTaskConfigsOffsetStorageModeSingle() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<String, String>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
            }
        };
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1, shards);
        assertThat(taskConfigs.size() == 1);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 4);
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), "task0_1");
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_SHARDS_CONFIG),
                String.join(",", shards));
        assertEquals(firstConfig.get("key"), "value");
    }

    @Test
    public void testTaskConfigsOffsetStorageModeDouble() {
        VitessConnector connector = new VitessConnector();
        Map<String, String> props = new HashMap<String, String>() {
            {
                put("key", "value");
                put(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name(), "true");
            }
        };
        connector.start(props);
        List<String> shards = Arrays.asList("-4000", "4000-8000", "8000-c000", "c000-");
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2, shards);
        assertThat(taskConfigs.size() == 2);
        Map<String, String> firstConfig = taskConfigs.get(0);
        assertThat(firstConfig.size() == 4);
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), "task0_2");
        assertEquals(firstConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_SHARDS_CONFIG), "-4000,8000-c000");
        assertEquals(firstConfig.get("key"), "value");
        Map<String, String> secondConfig = taskConfigs.get(1);
        assertThat(secondConfig.size() == 4);
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG), "task1_2");
        assertEquals(secondConfig.get(VitessConnectorConfig.VITESS_TASK_KEY_SHARDS_CONFIG), "4000-8000,c000-");
        assertEquals(secondConfig.get("key"), "value");
    }

    @Test
    public void testMultiTaskOnlyAllowedWithOffsetStoragePerTask() {
        Map<String, String> props = new HashMap<String, String>() {
            {
                put("key", "value");
                put("connector.class", "io.debezium.connector.vitess.VitessConnector");
                put("database.hostname", "host1");
                put("database.port", "15999");
                put("database.user", "vitess");
                put("database.password", "vitess-password");
                put("vitess.keyspace", "byuser");
                put("vitess.tablet.type", "MASTER");
                put("database.server.name", "dummy");
                put("message.key.columns", "c1");
                put(VitessConnectorConfig.TASKS_MAX_CONFIG, "2");
            }
        };
        VitessConnector connector = new VitessConnector();
        Configuration config = Configuration.from(props);
        Map<String, ConfigValue> results = connector.validateAllFields(config);
        LOGGER.info("results: {}", results);
        ConfigValue configValue = results.get(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name());
        assertThat(configValue != null);
        assertThat(configValue.errorMessages().size() == 1);
    }

    @Test
    public void testEmptyOffsetStorage() {
        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("current", "current"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("current", "current"));

        final Map<String, String> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks), vgtid1.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, null);
        Testing.print(String.format("vgtids: %s", vgtids));
        assertEquals(vgtids.size(), 2);
        assertEquals(vgtids.values().toArray(), expectedVgtids.values().toArray());
    }

    @Test
    public void testPreviousOffsetStorage() {
        final int numTasks = 2;
        final List<String> shards = Arrays.asList("s0", "s1", "s2", "s3");
        Vgtid vgtid0 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s0", "s2"), Arrays.asList("gt0", "gt2"));
        Vgtid vgtid1 = VitessReplicationConnection.buildVgtid(TEST_UNSHARDED_KEYSPACE,
                Arrays.asList("s1", "s3"), Arrays.asList("gt1", "gt3"));

        final Map<String, Object> expectedVgtids = Collect.hashMapOf(
                VitessConnector.getTaskKeyName(0, numTasks), vgtid0.toString(),
                VitessConnector.getTaskKeyName(1, numTasks), vgtid1.toString());
        Map<String, String> vgtids = getOffsetFromStorage(numTasks, shards, expectedVgtids);
        Testing.print(String.format("vgtids: %s", vgtids));
        assertEquals(vgtids.size(), 2);
        assertEquals(vgtids.values().toArray(), expectedVgtids.values().toArray());
    }

    private Map<String, String> getOffsetFromStorage(int numTasks, List<String> shards, Map<String, Object> prevVgtids) {
        final Configuration config = TestHelper.defaultConfig(false, true, numTasks).build();
        final OffsetBackingStore offsetStore = new MemoryOffsetBackingStore();
        offsetStore.start();
        final String engineName = "testOffset";
        final Converter keyConverter = new JsonConverter();
        Map<String, Object> converterConfig = Collect.hashMapOf("schemas.enable", false);
        keyConverter.configure(converterConfig, true);
        final Converter valueConverter = new JsonConverter();
        valueConverter.configure(converterConfig, false);
        if (prevVgtids != null) {
            Testing.print(String.format("Previous vgtids: %s", prevVgtids));
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, engineName,
                    keyConverter, valueConverter);
            for (String key : prevVgtids.keySet()) {
                Map<String, Object> sourcePartition = Collect.hashMapOf(
                        VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER,
                        VitessPartition.TASK_KEY_PARTITION_KEY, key);
                Map<String, Object> offset = new HashMap<>();
                offset.put(SourceInfo.VGTID_KEY, prevVgtids.get(key));
                offsetWriter.offset(sourcePartition, offset);
                offsetWriter.beginFlush();
                Future f = offsetWriter.doFlush(null);
                try {
                    f.get(100, TimeUnit.MILLISECONDS);
                }
                catch (Exception ex) {
                    fail(ex.getMessage());
                }
            }
        }
        final OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName,
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
        offsetStore.stop();
        return vgtids;
    }
}
