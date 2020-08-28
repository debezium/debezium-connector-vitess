/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

public class VitessConnectorTest {

    @Test
    public void shouldReturnConfigurationDefinition() {
        ConfigDef configDef = new VitessConnector().config();
        assertThat(configDef).isNotNull();
    }

    @Test
    public void shouldReturnVersion() {
        assertThat(new VitessConnector().version()).isNotNull();
    }
}
