/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Properties;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Collect;

/**
 * Topic naming strategy where only the table name is added. This is used to avoid including
 * the shard which is now part of the catalog of the table ID and would be included if
 * the DefaultTopicNamingStrategy is being used.
 */
public class TableTopicNamingStrategy extends AbstractTopicNamingStrategy<TableId> {

    public TableTopicNamingStrategy(Properties props) {
        super(props);
    }

    public static TableTopicNamingStrategy create(CommonConnectorConfig config) {
        return new TableTopicNamingStrategy(config.getConfig().asProperties());
    }

    @Override
    public String dataChangeTopic(TableId id) {
        String topicName = mkString(Collect.arrayListOf(prefix, id.table()), delimiter);
        return topicNames.computeIfAbsent(id, t -> sanitizedTopicName(topicName));
    }
}
