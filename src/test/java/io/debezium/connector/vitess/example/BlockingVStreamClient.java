/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.example;

import java.util.Iterator;
import java.util.List;

import io.vitess.client.grpc.StaticAuthCredentials;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import io.vitess.proto.grpc.VitessGrpc;

import binlogdata.Binlogdata;

/** Blocking client, if no response from server, it is blocked indefinitely. */
public class BlockingVStreamClient extends AbstractVStreamClient {

    public BlockingVStreamClient(
                                 String keyspace, List<String> shards, int gtidIdx, String host, String username, String password) {
        super(keyspace, shards, gtidIdx, host, username, password);
    }

    @Override
    public void startStreaming() throws InterruptedException {
        try {
            Binlogdata.VGtid vgtid = getPosition();

            VitessGrpc.VitessBlockingStub sub = VitessGrpc
                    .newBlockingStub(channel)
                    .withCallCredentials(new StaticAuthCredentials(username, password));

            while (true) {
                Iterator<Vtgate.VStreamResponse> response = sub.vStream(newVStreamRequest(vgtid, Topodata.TabletType.MASTER));
                while (response.hasNext()) {
                    processResponse(response.next());
                }
            }
        }
        finally {
            closeAndWait();
        }
    }
}
