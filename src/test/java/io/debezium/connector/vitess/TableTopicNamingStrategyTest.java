/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class TableTopicNamingStrategyTest {

    @Test
    public void shouldGetTopicNameWithoutShard() {
        TableId tableId = new TableId("shard", "keyspace", "table");
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", "prefix");
        TopicNamingStrategy strategy =  new TableTopicNamingStrategy(props);
        String topicName = strategy.dataChangeTopic(tableId);
        assertThat(topicName).isEqualTo("prefix.table");
    }

}