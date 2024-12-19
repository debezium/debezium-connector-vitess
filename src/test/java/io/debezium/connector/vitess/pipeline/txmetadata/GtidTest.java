/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.debezium.DebeziumException;

public class GtidTest {

    private static final String EXPECTED_VERSION = "MySQL56";
    private static final String HOST_SET1 = "/host1:1,host2:2-10";
    private static final String GTID1 = EXPECTED_VERSION + HOST_SET1;

    @Test
    public void shouldInit() {
        Gtid gtid = new Gtid(EXPECTED_VERSION + "/host1:1-4,host2:2-10");
        assertThat(gtid.getVersion()).isEqualTo(EXPECTED_VERSION);
        assertThat(gtid.getSequenceValues()).isEqualTo(List.of("4", "10"));
        assertThat(gtid.getHosts()).isEqualTo(Set.of("host1", "host2"));
    }

    @Test
    public void shouldHandleSingleValue() {
        Gtid gtid = new Gtid(GTID1);
        assertThat(gtid.getVersion()).isEqualTo(EXPECTED_VERSION);
        assertThat(gtid.getSequenceValues()).isEqualTo(List.of("1", "10"));
        assertThat(gtid.getHosts()).isEqualTo(Set.of("host1", "host2"));
    }

    @Test
    public void testHostSupersetWithLargerSet() {
        Gtid gtid = new Gtid(GTID1);
        Gtid gtidSuperset = new Gtid(EXPECTED_VERSION + "/host1:1,host2:2-10,host3:1-5");
        assertThat(gtidSuperset.isHostSetSupersetOf(gtid)).isTrue();
        assertThat(gtid.isHostSetSupersetOf(gtidSuperset)).isFalse();
    }

    @Test
    public void testHostSupersetWithEqualSet() {
        Gtid gtid = new Gtid(GTID1);
        Gtid gtid2 = new Gtid(GTID1);
        assertThat(gtid.isHostSetSupersetOf(gtid2)).isTrue();
        assertThat(gtid2.isHostSetSupersetOf(gtid)).isTrue();
    }

    @Test
    public void shouldThrowExceptionOnEmptyStringWithPrefix() {
        assertThatThrownBy(() -> {
            Gtid gtid = new Gtid(EXPECTED_VERSION + "/");
        }).isInstanceOf(DebeziumException.class);
    }

    @Test
    public void shouldThrowExceptionOnVersionOnly() {
        assertThatThrownBy(() -> {
            Gtid gtid = new Gtid(EXPECTED_VERSION);
        }).isInstanceOf(DebeziumException.class);
    }

    @Test
    public void shouldThrowExceptionOnVersionOnEmptyString() {
        assertThatThrownBy(() -> {
            Gtid gtid = new Gtid("");
        }).isInstanceOf(DebeziumException.class);
    }

}
