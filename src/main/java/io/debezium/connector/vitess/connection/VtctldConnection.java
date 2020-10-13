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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.Vgtid;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import binlogdata.Binlogdata;
import logutil.Logutil;
import vtctldata.Vtctldata;
import vtctlservice.VtctlGrpc;

/** Use VTCtld to do Vitess admin operations */
public class VtctldConnection implements AutoCloseable {

    /** The types of vitess tablet. */
    public enum TabletType {
        /** Master mysql instance. */
        MASTER,

        /** Replica slave, can be promoted to master. */
        REPLICA,

        /** Read only slave, can not be promoted to master. */
        RDONLY;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(VtctldConnection.class);

    // Used to retrieve the shard gtid from Vtctld response
    private static final int SHARD_GTID_INDEX = 8;
    // Flag used by ApplyVSchema command
    private static final String VSCHEMA_FLAG = "vschema";

    private final String vtctldHost;
    private final int vtctldPort;
    private final ManagedChannel managedChannel;

    private VtctldConnection(String vtctldHost, int vtctldPort) {
        this.vtctldHost = vtctldHost;
        this.vtctldPort = vtctldPort;
        this.managedChannel = ManagedChannelBuilder.forAddress(vtctldHost, vtctldPort).usePlaintext().build();
    }

    public static VtctldConnection of(String vtctldHost, int vtctldPort) {
        return new VtctldConnection(vtctldHost, vtctldPort);
    }

    /**
     * Get the latest VGTID position of a specific shard.
     *
     * @param keyspace
     * @param shard
     * @param tabletType
     * @return
     */
    public Vgtid latestVgtid(String keyspace, String shard, TabletType tabletType) {
        String command = "ShardReplicationPositions";
        List<String> args = Arrays.asList(command, keyspace + ":" + shard);

        List<String> results = execVtctl(args, vtctldHost, vtctldPort);
        LOGGER.info(
                "Get the latest replication positions of a specific keyspace {} shard {}: {}",
                keyspace,
                shard,
                results);
        String shardGtid = chooseShardGtid(results, tabletType);
        LOGGER.info("Choose ShardGtid: {}" + shardGtid);
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

    /**
     * Apply vschema to the keyspace
     *
     * @param vschema vschema in String
     * @param keyspace
     *
     * Throws runtime exception if the gRPC call fails.
     */
    public void applyVSchema(String vschema, String keyspace) {
        String command = "ApplyVSchema";
        List<String> args = Arrays.asList(command, "-" + VSCHEMA_FLAG + "=" + vschema, keyspace);
        List<String> results = execVtctl(args, vtctldHost, vtctldPort);
        LOGGER.info("Vschema {} is applied. Result: {}", vschema, results);
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

        VtctlGrpc.VtctlBlockingStub stub = VtctlGrpc.newBlockingStub(managedChannel);
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

        return res;
    }

    /** Close the gRPC connection */
    @Override
    public void close() throws Exception {
        LOGGER.info("Closing VTCtld connection");
        managedChannel.shutdownNow();
        LOGGER.trace("VTCtld GRPC channel shutdownNow is invoked.");
        if (managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.info("VTCtld GRPC channel is shutdown in time.");
        }
        else {
            LOGGER.warn("VTCtld GRPC channel is not shutdown in time. Give up waiting.");
        }
    }
}
