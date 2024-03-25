/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.junit.Test;

public class GtidTest {

    @Test
    public void shouldInit() {
        String expectedVersion = "MySQL56";
        Gtid gtid = new Gtid(expectedVersion + "/host1:1-4,host2:2-10");
        assertThat(gtid.getVersion()).isEqualTo(expectedVersion);
        assertThat(gtid.getSequenceValues()).isEqualTo(List.of("4", "10"));
        assertThat(gtid.getHosts()).isEqualTo(Set.of("host1", "host2"));
    }

}
