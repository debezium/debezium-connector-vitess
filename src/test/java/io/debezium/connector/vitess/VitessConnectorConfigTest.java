/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;

public class VitessConnectorConfigTest {

    @Test
    public void shouldGetVitessHeartbeatImplWhenIntervalSet() {
        Configuration configuration = TestHelper.defaultConfig().with(
                Heartbeat.HEARTBEAT_INTERVAL, 1000).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        Heartbeat heartbeat = connectorConfig.createHeartbeat(
                DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.NO_OP,
                null,
                null);
        assertThat(heartbeat).isNotNull();
        assertThat(heartbeat instanceof VitessHeartbeatImpl).isTrue();
    }

    @Test
    public void shouldGetVitessHeartbeatNoOp() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        Heartbeat heartbeat = connectorConfig.createHeartbeat(
                DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.NO_OP,
                null,
                null);
        assertThat(heartbeat).isNotNull();
        assertThat(heartbeat).isEqualTo(Heartbeat.DEFAULT_NOOP_HEARTBEAT);
    }

}
