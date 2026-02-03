/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.vitess.Vgtid;

/**
 * A Vitess logical streaming replication connection. Replication connections are established from a
 * vtgate, starting from a specific {@link Vgtid}.
 */
@NotThreadSafe
public interface ReplicationConnection extends AutoCloseable {

    /**
     * Opens a stream that reads from a specific replication position.
     *
     * @param vgtid a specific replication position
     * @param processor - a callback to which the decoded message is passed
     * @param error - check whether an error has happened during streaming, propagate the error
     *     asynchronously
     */
    void startStreaming(
                        Vgtid vgtid, ReplicationMessageProcessor processor, AtomicReference<Throwable> error);

    /**
     * Returns true if the copy phase (VStream Copy) has completed.
     * This is used to determine when to stop the connector in initial_only mode.
     *
     * @return true if copy phase is complete, false otherwise
     */
    boolean isCopyCompleted();
}
