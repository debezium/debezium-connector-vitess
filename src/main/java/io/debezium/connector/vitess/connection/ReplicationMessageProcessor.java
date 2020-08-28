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
     */
    void process(ReplicationMessage message, Vgtid newVgtid) throws InterruptedException;
}
