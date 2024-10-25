/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Test;

import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;

public class TableTopicNamingStrategyTest {

    @Test
    public void shouldGetTopicNameWithoutShard() {
        TableId tableId = new TableId("shard", "keyspace", "table");
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", "prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.dataChangeTopic(tableId);
        assertThat(topicName).isEqualTo("prefix.table");
    }

    @Test
    public void shouldGetOverrideDataChangeTopic() {
        TableId tableId = new TableId("shard", "keyspace", "table");
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", "prefix");
        props.put(VitessConnectorConfig.OVERRIDE_DATA_CHANGE_TOPIC_PREFIX.name(), "override-prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.dataChangeTopic(tableId);
        assertThat(topicName).isEqualTo("override-prefix.table");
    }

    @Test
    public void shouldUseTopicPrefixIfOverrideIsBlank() {
        TableId tableId = new TableId("shard", "keyspace", "table");
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", "prefix");
        props.put(VitessConnectorConfig.OVERRIDE_DATA_CHANGE_TOPIC_PREFIX.name(), "");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.dataChangeTopic(tableId);
        assertThat(topicName).isEqualTo("prefix.table");
    }

    @Test
    public void shouldGetOverrideSchemaChangeTopic() {
        TableId tableId = new TableId("shard", "keyspace", "table");
        final Properties props = new Properties();
        props.put("topic.prefix", "prefix");
        props.put(VitessConnectorConfig.OVERRIDE_SCHEMA_CHANGE_TOPIC.name(), "override-prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.schemaChangeTopic();
        assertThat(topicName).isEqualTo("override-prefix");
    }

    @Test
    public void shouldUseTopicPrefixIfOverrideSchemaIsBlank() {
        TableId tableId = new TableId("shard", "keyspace", "table");
        final Properties props = new Properties();
        props.put("topic.prefix", "prefix");
        props.put(VitessConnectorConfig.OVERRIDE_SCHEMA_CHANGE_TOPIC.name(), "");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.schemaChangeTopic();
        assertThat(topicName).isEqualTo("prefix");
    }

}
