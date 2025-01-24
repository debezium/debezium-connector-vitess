/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.Test;

import io.debezium.connector.vitess.TestHelper;

/**
 * @author Thomas Thornton
 */
public class TransactionalMessageTest {

    @Test
    public void shouldInstantiateBeginMessage() {
        ReplicationMessage message = new TransactionalMessage(
                ReplicationMessage.Operation.BEGIN, "tx_id", Instant.now(), TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        assertThat(message.isTransactionalMessage()).isTrue();
        assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.BEGIN);
    }

    @Test
    public void shouldInstantiateCommitMessage() {
        ReplicationMessage message = new TransactionalMessage(
                ReplicationMessage.Operation.COMMIT, "tx_id", Instant.now(), TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        assertThat(message.isTransactionalMessage()).isTrue();
        assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.COMMIT);
    }

    @Test
    public void shouldThrowExceptionForNonTransactionalOperation() {
        assertThatThrownBy(() -> {
            new TransactionalMessage(
                    ReplicationMessage.Operation.INSERT, "tx_id", Instant.now(), TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TransactionalMessage can only have BEGIN or COMMIT operations");
    }
}
