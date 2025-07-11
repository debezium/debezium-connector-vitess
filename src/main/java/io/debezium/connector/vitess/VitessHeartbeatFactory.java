/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static io.debezium.config.CommonConnectorConfig.TOPIC_NAMING_STRATEGY;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.DebeziumHeartbeatFactory;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.heartbeat.HeartbeatConnectionProvider;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.pipeline.DataChangeEvent;

public class VitessHeartbeatFactory implements DebeziumHeartbeatFactory {
    @Override
    public ScheduledHeartbeat getScheduledHeartbeat(CommonConnectorConfig connectorConfig, HeartbeatConnectionProvider connectionProvider,
                                                    HeartbeatErrorHandler errorHandler, ChangeEventQueue<DataChangeEvent> queue) {
        if (connectorConfig.getHeartbeatInterval().isZero()) {
            return ScheduledHeartbeat.NOOP_HEARTBEAT;
        }
        return new VitessHeartbeatImpl(connectorConfig.getHeartbeatInterval(),
                connectorConfig.getTopicNamingStrategy(TOPIC_NAMING_STRATEGY).heartbeatTopic(),
                connectorConfig.getLogicalName(),
                connectorConfig.schemaNameAdjuster(), queue);
    }
}
