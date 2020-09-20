/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import io.vitess.proto.grpc.VitessGrpc;

import binlogdata.Binlogdata;
import binlogdata.Binlogdata.VEvent;

/**
 * Connection to VTGate to replication messages. Also connect to VTCtld to get the latest {@link
 * Vgtid} if no previous offset exists.
 */
public class VitessReplicationConnection implements ReplicationConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessReplicationConnection.class);

    private final MessageDecoder messageDecoder;
    private final VitessConnectorConfig config;
    // Channel closing is invoked from the change-event-source-coordinator thread
    private final AtomicReference<ManagedChannel> managedChannel = new AtomicReference<>();

    public VitessReplicationConnection(VitessConnectorConfig config, VitessDatabaseSchema schema) {
        this.messageDecoder = new VStreamOutputMessageDecoder(schema);
        this.config = config;
    }

    @Override
    public void startStreaming(
                               Vgtid vgtid, ReplicationMessageProcessor processor, AtomicReference<Throwable> error) {
        if (vgtid == null) {
            Objects.requireNonNull(vgtid);
        }

        ManagedChannel channel = ManagedChannelBuilder.forAddress(config.getVtgateHost(), config.getVtgatePort())
                .usePlaintext()
                .build();
        managedChannel.compareAndSet(null, channel);

        // Providing a vgtid MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-61 here will make VStream to
        // start receiving
        // row-changes from MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-62
        VitessGrpc.VitessStub stub = VitessGrpc.newStub(channel);
        StreamObserver<Vtgate.VStreamResponse> responseObserver = new StreamObserver<Vtgate.VStreamResponse>() {

            @Override
            public void onNext(Vtgate.VStreamResponse response) {
                LOGGER.debug("Received {} vEvents in the VStreamResponse:",
                        response.getEventsCount());
                for (VEvent vEvent : response.getEventsList()) {
                    LOGGER.debug("vEvent: {}", vEvent);
                }

                Vgtid newVgtid = getVgtid(response);

                try {
                    for (VEvent vEvent : response.getEventsList()) {
                        messageDecoder.processMessage(vEvent, processor, newVgtid);
                    }
                }
                catch (InterruptedException e) {
                    LOGGER.error("Message processing is interrupted", e);
                    // Only propagate the first error
                    error.compareAndSet(null, e);
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.info("VStream streaming onError. Status: " + Status.fromThrowable(t), t);
                // Only propagate the first error
                error.compareAndSet(null, t);
            }

            @Override
            public void onCompleted() {
                LOGGER.info("VStream streaming completed.");
            }

            // We assume there is only one vgtid event for response.
            // Even in case of resharding, there is only one vgtid event that contains multiple shard
            // gtids.
            private Vgtid getVgtid(Vtgate.VStreamResponse response) {
                LinkedList<Vgtid> vgtids = new LinkedList<>();
                for (VEvent vEvent : response.getEventsList()) {
                    if (vEvent.getType() == Binlogdata.VEventType.VGTID) {
                        vgtids.addLast(Vgtid.of(vEvent.getVgtid()));
                    }
                }
                if (vgtids.size() == 0) {
                    // The VStreamResponse that contains an VERSION vEvent does not have VGTID.
                    // We do not update lastReceivedVgtid in this case.
                    // It can also be null if the 1st grpc response does not have vgtid upon restart
                    LOGGER.warn("No vgtid found in response {}...", response.toString().substring(0, 100));
                    LOGGER.debug("Response is {}", response);
                    return null;
                }
                if (vgtids.size() > 1) {
                    LOGGER.error(
                            "Should only have 1 vgtid per VStreamResponse, but found {}. Use the last vgtid.",
                            vgtids.size());
                }
                return vgtids.getLast();
            }
        };

        stub.vStream(
                Vtgate.VStreamRequest.newBuilder()
                        .setVgtid(vgtid.getRawVgtid())
                        .setTabletType(
                                toTopodataTabletType(VgtidReader.TabletType.valueOf(config.getTabletType())))
                        .build(),
                responseObserver);
    }

    /** Close the gRPC connection to VStream */
    @Override
    public void close() throws Exception {
        LOGGER.info("Closing replication connection");
        managedChannel.get().shutdownNow();
        LOGGER.trace("VStream GRPC channel shutdownNow is invoked.");
        if (managedChannel.get().awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.info("VStream GRPC channel is shutdown in time.");
        }
        else {
            LOGGER.warn("VStream GRPC channel is not shutdown in time. Give up waiting.");
        }
    }

    /** Get latest replication position */
    public static Vgtid defaultVgtid(VitessConnectorConfig config) {
        return VtctldVgtidReader.of(config.getVtctldHost(), config.getVtctldPort())
                .latestVgtid(
                        config.getKeyspace(),
                        config.getShard(),
                        VgtidReader.TabletType.valueOf(config.getTabletType()));
    }

    private static Topodata.TabletType toTopodataTabletType(VgtidReader.TabletType tabletType) {
        switch (tabletType) {
            case MASTER:
                return Topodata.TabletType.MASTER;
            case REPLICA:
                return Topodata.TabletType.REPLICA;
            case RDONLY:
                return Topodata.TabletType.RDONLY;
            default:
                LOGGER.warn("Unknown tabletType {}", tabletType);
                return null;
        }
    }
}
