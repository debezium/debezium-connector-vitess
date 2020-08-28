/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * The topic naming strategy based on connector configuration and table name. Same behavior as
 * Debezium topic selector.
 */
public class VitessTopicSelector {

    public static TopicSelector<TableId> defaultSelector(VitessConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(
                connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }
}
