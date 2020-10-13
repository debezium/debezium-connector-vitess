/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import io.debezium.connector.vitess.Vgtid;

@FunctionalInterface
public interface ReplicationMessageProcessor {
    /**
     * Processes the given replication message.
     *
     * @param message The replication message, never {@code null}.
     * @param newVgtid The VGTID in the gRPC response, can be {@code null}.
     * @param isLastRowOfTransaction Whether the message is the last row change of the last row event of the transaction.
     */
    void process(ReplicationMessage message, Vgtid newVgtid, boolean isLastRowOfTransaction) throws InterruptedException;
}
