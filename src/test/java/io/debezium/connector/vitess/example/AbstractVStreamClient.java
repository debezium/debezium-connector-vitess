/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.example;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.vitess.client.grpc.StaticAuthCredentials;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;

import binlogdata.Binlogdata;
import logutil.Logutil;
import vtctldata.Vtctldata;
import vtctlservice.VtctlGrpc;

public abstract class AbstractVStreamClient {
    private static final java.util.logging.Logger LOGGER = Logger.getLogger(AbstractVStreamClient.class.getName());

    // vtgate grpc server
    private static int VTGATE_PORT = 15991;

    // vtctld grpc server
    private static int VTCTLD_PORT = 15999;

    private static String CURRENT_GTID = "current";

    private final String keyspace;
    private final List<String> shards;
    private final int gtidIdx;
    private final String host;
    protected final String username;
    protected final String password;

    protected final ManagedChannel channel;

    public AbstractVStreamClient(
                                 String keyspace, List<String> shards, int gtidIdx, String host, String username, String password) {
        this.keyspace = keyspace;
        this.shards = shards;
        this.gtidIdx = gtidIdx;
        this.host = host;
        this.username = username;
        this.password = password;
        this.channel = newChannel();
    }

    protected void closeAndWait() throws InterruptedException {
        channel.shutdownNow();
        LOGGER.info("Channel shutdownNow is invoked.");
        if (channel.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.info("Channel is shut down in time. Exiting.");
        }
        else {
            LOGGER.warning("Give up waiting for channel shutdownNow. Exiting.");
        }
    }

    /**
     * Start streaming from VTGate. It is blocked until one of the following condition happens:
     *
     * <p>1. Server returns error.
     *
     * <p>2. Some runtime error happens when processing the response on the client side.
     *
     * @throws InterruptedException
     */
    public abstract void startStreaming() throws InterruptedException;

    protected Binlogdata.VGtid processResponse(Vtgate.VStreamResponse response) {
        Binlogdata.VGtid vGtid = null;
        LOGGER.info("======response======");
        for (Binlogdata.VEvent event : response.getEventsList()) {
            if (event.getVgtid().getShardGtidsCount() != 0) {
                vGtid = event.getVgtid();
            }
            LOGGER.info(event.toString());
        }

        return vGtid;
    }

    protected static Vtgate.VStreamRequest newVStreamRequest(
                                                             Binlogdata.VGtid vgtid, Topodata.TabletType tabletType) {
        return Vtgate.VStreamRequest.newBuilder().setVgtid(vgtid).setTabletType(tabletType).build();
    }

    protected ManagedChannel newChannel() {
        return ManagedChannelBuilder.forAddress(host, VTGATE_PORT).usePlaintext().build();
    }

    // Get current vgtid position of a specific keyspace/shard, or of all shards from a specific keyspace
    protected Binlogdata.VGtid getPosition() {
        Binlogdata.VGtid.Builder builder = Binlogdata.VGtid.newBuilder();
        if (shards.isEmpty()) {
            Binlogdata.ShardGtid shardGtid = Binlogdata.ShardGtid.newBuilder()
                    .setKeyspace(keyspace)
                    .setGtid(CURRENT_GTID)
                    .build();
            return builder.addShardGtids(shardGtid).build();
        }
        else {
            for (String shard : shards) {
                builder.addShardGtids(getShardGtid(shard));
            }
            return builder.build();
        }
    }

    protected Binlogdata.ShardGtid getShardGtid(String shard) {
        List<String> args = new ArrayList<>();
        args.add("ShardReplicationPositions");
        args.add(keyspace + ":" + shard);
        List<String> results = execVtctl(args, host, VTCTLD_PORT);

        // String shardGtid = "MariaDB/0-1591716567-305";
        String shardGtid = results.get(0).split(" ")[gtidIdx];
        LOGGER.log(Level.INFO, "ShardGtid: {0}", shardGtid);
        return Binlogdata.ShardGtid.newBuilder()
                .setKeyspace(keyspace)
                .setShard(shard)
                .setGtid(shardGtid)
                .build();
    }

    private List<String> execVtctl(List<String> args, String vtctldHost, int vtctldPort) {
        List<String> res = new ArrayList<>();

        ManagedChannel channel = ManagedChannelBuilder.forAddress(vtctldHost, vtctldPort).usePlaintext().build();

        VtctlGrpc.VtctlBlockingStub stub = VtctlGrpc
                .newBlockingStub(channel)
                .withCallCredentials(new StaticAuthCredentials(username, password));
        Iterator<Vtctldata.ExecuteVtctlCommandResponse> responseIter = stub.executeVtctlCommand(
                Vtctldata.ExecuteVtctlCommandRequest.newBuilder()
                        .setActionTimeout(10_000_000_000L) // 10 seconds in nano-seconds
                        .addArgs(args.get(0))
                        .addArgs(args.get(1))
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
