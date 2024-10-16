/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.Test;

/**
 * @author Thomas Thornton
 */
public class DdlMessageTest {

    @Test
    public void shouldSetQuery() {
        String statement = "ALTER TABLE foo RENAME TO bar";
        ReplicationMessage replicationMessage = new DdlMessage("gtid", Instant.EPOCH, statement, "0");
        assertThat(replicationMessage.getStatement()).isEqualTo(statement);
    }

    @Test
    public void shouldSetShard() {
        String statement = "ALTER TABLE foo RENAME TO bar";
        String shard = "-80";
        ReplicationMessage replicationMessage = new DdlMessage("gtid", Instant.EPOCH, statement, shard);
        assertThat(replicationMessage.getShard()).isEqualTo(shard);
    }

    @Test
    public void shouldConvertToString() {
        String statement = "ALTER TABLE foo RENAME TO bar";
        String shard = "-80";
        ReplicationMessage replicationMessage = new DdlMessage("gtid", Instant.EPOCH, statement, shard);
        assertThat(replicationMessage.toString()).isEqualTo(
                "DdlMessage{transactionId='gtid', shard=-80, commitTime=1970-01-01T00:00:00Z, statement=ALTER TABLE foo RENAME TO bar, operation=DDL}");
    }

}
