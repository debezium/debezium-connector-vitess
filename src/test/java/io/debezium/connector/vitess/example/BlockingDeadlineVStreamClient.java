/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.example;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.vitess.client.grpc.StaticAuthCredentials;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import io.vitess.proto.grpc.VitessGrpc;

import binlogdata.Binlogdata;

/** Blocking client, but if no response from server, timeout and retry. */
public class BlockingDeadlineVStreamClient extends AbstractVStreamClient {
    private static final java.util.logging.Logger LOGGER = Logger.getLogger(BlockingDeadlineVStreamClient.class.getName());

    private final int deadlineInSeconds;

    public BlockingDeadlineVStreamClient(
                                         int deadlineInSeconds, String keyspace, List<String> shards, int gtidIdx, String host, String username, String password) {
        super(keyspace, shards, gtidIdx, host, username, password);
        this.deadlineInSeconds = deadlineInSeconds;
    }

    @Override
    public void startStreaming() throws InterruptedException {
        try {
            Binlogdata.VGtid vgtid = getPosition();

            VitessGrpc.VitessBlockingStub sub = VitessGrpc
                    .newBlockingStub(channel)
                    .withCallCredentials(new StaticAuthCredentials(username, password));

            while (true) {
                Iterator<Vtgate.VStreamResponse> response = sub.withDeadlineAfter(deadlineInSeconds, TimeUnit.SECONDS)
                        .vStream(newVStreamRequest(vgtid, Topodata.TabletType.MASTER));
                try {
                    while (response.hasNext()) {
                        vgtid = processResponse(response.next());
                    }
                }
                catch (StatusRuntimeException e) {
                    Status status = e.getStatus();
                    if (status.getCode().equals(Status.DEADLINE_EXCEEDED.getCode())) {
                        LOGGER.info("Deadline exceeded, try again.");
                    }
                    else {
                        throw e;
                    }
                }
            }
        }
        finally {
            closeAndWait();
        }
    }
}
