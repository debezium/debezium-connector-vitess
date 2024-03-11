/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import io.debezium.connector.vitess.Vgtid;

import binlogdata.Binlogdata;

import java.time.Instant;

/** Decode VStream gRPC VEvent and process it with the ReplicationMessageProcessor. */
public interface MessageDecoder {

    void processMessage(Binlogdata.VEvent event, ReplicationMessageProcessor processor, Vgtid newVgtid, boolean isLastRowEventOfTransaction)
            throws InterruptedException;

    void setCommitTimestamp(Instant commitTimestamp);
}
