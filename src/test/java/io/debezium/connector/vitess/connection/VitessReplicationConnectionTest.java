/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.VitessConnectorConfig;

public class VitessReplicationConnectionTest {

    @Test
    public void shouldQuoteIdentifier() {
        VitessReplicationConnection connection = new VitessReplicationConnection(
                new VitessConnectorConfig(TestHelper.defaultConfig().build()), null);
        assertThat(connection.quoteIdentifier("keyspace")).isEqualTo("`keyspace`");
        assertThat(connection.quoteIdentifier("tenant-a")).isEqualTo("`tenant-a`");
        assertThat(connection.quoteIdentifier("weird`name")).isEqualTo("`weird``name`");
    }
}
