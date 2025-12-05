/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

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
        props.put(TableTopicNamingStrategy.OVERRIDE_DATA_CHANGE_TOPIC_PREFIX.name(), "override-prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.dataChangeTopic(tableId);
        assertThat(topicName).isEqualTo("override-prefix.table");
    }

    @Test
    public void shouldGetOverrideDataChangeTopicForHeartbeatTable() {
        TableId tableId = new TableId("shard", "keyspace", "heartbeat");
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", "prefix");
        props.put(TableTopicNamingStrategy.OVERRIDE_DATA_CHANGE_TOPIC_PREFIX_EXCLUDE_LIST.name(), "keyspace.heartbeat");
        props.put(TableTopicNamingStrategy.OVERRIDE_DATA_CHANGE_TOPIC_PREFIX.name(), "override-prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.dataChangeTopic(tableId);
        assertThat(topicName).isEqualTo("prefix.heartbeat");
    }

    @Test
    public void shouldUseTopicPrefixIfOverrideIsNotSpecified() {
        TableId tableId = new TableId("shard", "keyspace", "table");
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", "prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.dataChangeTopic(tableId);
        assertThat(topicName).isEqualTo("prefix.table");
    }

    @Test
    public void shouldGetOverrideSchemaChangeTopic() {
        final Properties props = new Properties();
        props.put("topic.prefix", "prefix");
        props.put(TableTopicNamingStrategy.OVERRIDE_SCHEMA_CHANGE_TOPIC.name(), "override-prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.schemaChangeTopic();
        assertThat(topicName).isEqualTo("override-prefix");
    }

    @Test
    public void shouldUseTopicPrefixIfOverrideSchemaIsNotSpecified() {
        final Properties props = new Properties();
        props.put("topic.prefix", "prefix");
        TopicNamingStrategy strategy = new TableTopicNamingStrategy(props);
        String topicName = strategy.schemaChangeTopic();
        assertThat(topicName).isEqualTo("prefix");
    }

    @Test
    public void shouldUseValidationsOfParentClass() {
        final Properties props = new Properties();
        props.put("topic.prefix", "invalid?.topic.name");
        assertThatThrownBy(() -> {
            new TableTopicNamingStrategy(props);
        }).isInstanceOf(ConnectException.class);
    }

    @Test
    public void shouldThrowExceptionForInvalidOverrideDataChangeTopic() {
        final Properties props = new Properties();
        props.put("topic.prefix", "dev.database");
        props.put(TableTopicNamingStrategy.OVERRIDE_DATA_CHANGE_TOPIC_PREFIX.name(), "invalid?.topic.name");
        assertThatThrownBy(() -> {
            new TableTopicNamingStrategy(props);
        }).isInstanceOf(ConnectException.class);
    }

    @Test
    public void shouldBlankOverrideTopicPrefixFailValidation() {
        final Properties props = new Properties();
        props.put("topic.prefix", "dev.database");
        props.put(TableTopicNamingStrategy.OVERRIDE_DATA_CHANGE_TOPIC_PREFIX.name(), "");
        assertThatThrownBy(() -> {
            new TableTopicNamingStrategy(props);
        }).isInstanceOf(ConnectException.class);
    }

    @Test
    public void shouldThrowExceptionForInvalidOverrideSchemaChangeTopic() {
        final Properties props = new Properties();
        props.put("topic.prefix", "dev.database");
        props.put(TableTopicNamingStrategy.OVERRIDE_SCHEMA_CHANGE_TOPIC.name(), "invalid?.topic.name");
        assertThatThrownBy(() -> {
            new TableTopicNamingStrategy(props);
        }).isInstanceOf(ConnectException.class);
    }

    @Test
    public void shouldBlankOverrideSchemaTopicPrefixFailValidation() {
        final Properties props = new Properties();
        props.put("topic.prefix", "dev.database");
        props.put(TableTopicNamingStrategy.OVERRIDE_SCHEMA_CHANGE_TOPIC.name(), "");
        assertThatThrownBy(() -> {
            new TableTopicNamingStrategy(props);
        }).isInstanceOf(ConnectException.class);
    }

}
