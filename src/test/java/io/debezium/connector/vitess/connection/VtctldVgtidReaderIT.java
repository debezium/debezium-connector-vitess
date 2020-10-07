/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;

public class VtctldVgtidReaderIT {

    @Test
    public void shouldGetValidVgtid() {
        List<String> tabletTypes = Arrays.asList("MASTER", "REPLICA", "RDONLY");
        tabletTypes.stream().forEach(this::assertGetValidVgtid);
    }

    private void assertGetValidVgtid(String tabletType) {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(
                TestHelper.defaultConfig().with(VitessConnectorConfig.TABLET_TYPE, tabletType).build());
        VtctldVgtidReader reader = VtctldVgtidReader.of(conf.getVtctldHost(), conf.getVtctldPort());

        // exercise SUT
        Vgtid vgtid = reader.latestVgtid(
                conf.getKeyspace(),
                conf.getShard(),
                VgtidReader.TabletType.valueOf(conf.getTabletType()));

        // verify outcome
        assertThat(vgtid).isNotNull();
        assertThat(vgtid.getShardGtids()).hasSize(1);
        assertThat(vgtid.getShardGtids().iterator().next().getKeyspace()).isEqualTo(conf.getKeyspace());
        assertThat(vgtid.getShardGtids().iterator().next().getShard()).isEqualTo(conf.getShard());
        String gtid = vgtid.getShardGtids().iterator().next().getGtid();
        assertThat(gtid.startsWith("MySQL") || gtid.startsWith("MariaDB")).isTrue();
    }
}
