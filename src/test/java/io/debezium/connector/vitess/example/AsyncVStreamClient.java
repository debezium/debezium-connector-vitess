/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.example;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.stub.StreamObserver;
import io.vitess.client.grpc.StaticAuthCredentials;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import io.vitess.proto.grpc.VitessGrpc;

import binlogdata.Binlogdata;

/**
 * Non-blocking, asynchronous client. Artificially block with timeoutMinutes to prevent existing.
 */
public class AsyncVStreamClient extends AbstractVStreamClient {
    private static final java.util.logging.Logger LOGGER = Logger.getLogger(AsyncVStreamClient.class.getName());

    private final int timeoutMinutes;

    public AsyncVStreamClient(
                              int timeoutMinutes, String keyspace, List<String> shards, int gtidIdx, String host, String username, String password) {
        super(keyspace, shards, gtidIdx, host, username, password);
        this.timeoutMinutes = timeoutMinutes;
    }

    @Override
    public void startStreaming() throws InterruptedException {
        CountDownLatch finishLatch = new CountDownLatch(1);
        try {
            Binlogdata.VGtid vgtid = getPosition();

            VitessGrpc.VitessStub sub = VitessGrpc
                    .newStub(channel)
                    .withCallCredentials(new StaticAuthCredentials(username, password));

            sub.vStream(
                    newVStreamRequest(vgtid, Topodata.TabletType.MASTER), newStreamObserver(finishLatch));

            if (!finishLatch.await(timeoutMinutes, TimeUnit.MINUTES)) {
                LOGGER.info("Stream did not finish in time.");
            }
        }
        finally {
            closeAndWait();
        }
    }

    private StreamObserver<Vtgate.VStreamResponse> newStreamObserver(CountDownLatch finishLatch) {
        StreamObserver<Vtgate.VStreamResponse> responseObserver = new StreamObserver<Vtgate.VStreamResponse>() {
            @Override
            public void onNext(Vtgate.VStreamResponse response) {
                processResponse(response);
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.log(Level.SEVERE, "StreamObserver onError is invoked.", t);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.info("Stream is completed.");
                finishLatch.countDown();
            }
        };
        return responseObserver;
    }
}
