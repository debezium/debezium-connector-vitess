/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

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
}
