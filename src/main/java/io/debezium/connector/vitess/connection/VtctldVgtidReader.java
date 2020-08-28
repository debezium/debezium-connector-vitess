/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.Vgtid;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import binlogdata.Binlogdata;
import logutil.Logutil;
import vtctldata.Vtctldata;
import vtctlservice.VtctlGrpc;

/** Use VTCtld to look up VGTID of a specific shard */
public class VtctldVgtidReader implements VgtidReader {
    private static final Logger log = LoggerFactory.getLogger(VtctldVgtidReader.class);

    // Used to retrieve the shard gtid from Vtctld response
    private static final int SHARD_GTID_INDEX = 8;

    private final String vtctldHost;
    private final int vtctldPort;

    private VtctldVgtidReader(String vtctldHost, int vtctldPort) {
        this.vtctldHost = vtctldHost;
        this.vtctldPort = vtctldPort;
    }

    public static VtctldVgtidReader of(String vtctldHost, int vtctldPort) {
        return new VtctldVgtidReader(vtctldHost, vtctldPort);
    }

    @Override
    public Vgtid latestVgtid(String keyspace, String shard, TabletType tabletType) {
        String COMMAND = "ShardReplicationPositions";
        List<String> args = Arrays.asList(COMMAND, keyspace + ":" + shard);

        List<String> results = execVtctl(args, vtctldHost, vtctldPort);
        log.info(
                "Get the latest replication positions of a specific keyspace {} shard {}: {}",
                keyspace,
                shard,
                results);
        String shardGtid = chooseShardGtid(results, tabletType);
        log.info("Choose ShardGtid: {}" + shardGtid);
        return Vgtid.of(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(
                                Binlogdata.ShardGtid.newBuilder()
                                        .setKeyspace(keyspace)
                                        .setShard(shard)
                                        .setGtid(shardGtid)
                                        .build())
                        .build());
    }

    private String chooseShardGtid(List<String> results, TabletType tabletType) {
        int tabletTypeIdx = 0;
        switch (tabletType) {
            case MASTER:
                tabletTypeIdx = 0;
                break;
            case REPLICA:
                tabletTypeIdx = 1;
                break;
            case RDONLY:
                tabletTypeIdx = 2;
                break;
        }
        return results.get(tabletTypeIdx).split(" ")[SHARD_GTID_INDEX];
    }

    private List<String> execVtctl(List<String> args, String vtctldHost, int vtctldPort) {
        List<String> res = new ArrayList<>();

        ManagedChannel channel = ManagedChannelBuilder.forAddress(vtctldHost, vtctldPort).usePlaintext().build();

        VtctlGrpc.VtctlBlockingStub stub = VtctlGrpc.newBlockingStub(channel);
        Iterator<Vtctldata.ExecuteVtctlCommandResponse> responseIter = stub.executeVtctlCommand(
                Vtctldata.ExecuteVtctlCommandRequest.newBuilder()
                        .setActionTimeout(10_000_000_000L) // 10 seconds in nano-seconds
                        .addAllArgs(args)
                        .build());

        while (responseIter.hasNext()) {
            Vtctldata.ExecuteVtctlCommandResponse response = responseIter.next();
            Logutil.Event event = response.getEvent();
            if (Logutil.Level.CONSOLE.equals(event.getLevel())) {
                res.add(event.getValue());
            }
        }

        channel.shutdown();
        return res;
    }
}
