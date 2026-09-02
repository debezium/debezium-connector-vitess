/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.doc.FixFor;

public class VitessReplicationConnectionTest {

    @Test
    public void shouldQuoteIdentifier() {
        VitessReplicationConnection connection = new VitessReplicationConnection(
                new VitessConnectorConfig(TestHelper.defaultConfig().build()), null);
        assertThat(connection.quoteIdentifier("keyspace")).isEqualTo("`keyspace`");
        assertThat(connection.quoteIdentifier("tenant-a")).isEqualTo("`tenant-a`");
        assertThat(connection.quoteIdentifier("weird`name")).isEqualTo("`weird``name`");
    }

    @Test
    @FixFor("debezium/dbz#2545")
    public void shouldCloseWithoutEverConnecting() {
        VitessReplicationConnection connection = new VitessReplicationConnection(
                new VitessConnectorConfig(TestHelper.defaultConfig().build()), null);
        assertThatNoException().isThrownBy(connection::close);
    }

    @Test
    @FixFor("debezium/dbz#2546")
    public void shouldBuildVgtidFromExplicitEmptyVgtidWithShards() {
        // Build the empty string at runtime so it is an equal-but-distinct instance of
        // Vgtid.EMPTY_GTID, like a value parsed from user configuration.
        String explicitEmptyVgtid = new StringBuilder().toString();
        VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig()
                .with(VitessConnectorConfig.SNAPSHOT_MODE, VitessConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(VitessConnectorConfig.SHARD, "-80")
                .with(VitessConnectorConfig.VGTID, explicitEmptyVgtid)
                .build());

        Vgtid vgtid = VitessReplicationConnection.defaultVgtid(config);

        assertThat(vgtid.getShardGtids()).hasSize(1);
        assertThat(vgtid.getShardGtids().get(0).getShard()).isEqualTo("-80");
        assertThat(vgtid.getShardGtids().get(0).getGtid()).isEqualTo(Vgtid.EMPTY_GTID);
    }

}
