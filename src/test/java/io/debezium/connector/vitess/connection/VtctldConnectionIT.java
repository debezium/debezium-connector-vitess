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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;

public class VtctldConnectionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(VtctldConnectionIT.class);

    @Test
    public void shouldGetValidVgtid() throws Exception {
        List<String> tabletTypes = Arrays.asList("MASTER", "REPLICA", "RDONLY");
        for (String tabletType : tabletTypes) {
            assertGetValidVgtid(tabletType);
        }
    }

    private void assertGetValidVgtid(String tabletType) throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(
                TestHelper.defaultConfig().with(VitessConnectorConfig.TABLET_TYPE, tabletType).build());

        try (VtctldConnection vtctldConnection = VtctldConnection.of(
                conf.getVtctldHost(), conf.getVtctldPort(), conf.getVtctldUsername(), conf.getVtctldPassword())) {
            // exercise SUT
            Vgtid vgtid = vtctldConnection.latestVgtid(
                    conf.getKeyspace(),
                    conf.getShard(),
                    VtctldConnection.TabletType.valueOf(conf.getTabletType()));

            // verify outcome
            assertThat(vgtid).isNotNull();
            assertThat(vgtid.getShardGtids()).hasSize(1);
            assertThat(vgtid.getShardGtids().iterator().next().getKeyspace()).isEqualTo(conf.getKeyspace());
            assertThat(vgtid.getShardGtids().iterator().next().getShard()).isEqualTo(conf.getShard());
            String gtid = vgtid.getShardGtids().iterator().next().getGtid();
            assertThat(gtid.startsWith("MySQL") || gtid.startsWith("MariaDB")).isTrue();
        }
    }
}
