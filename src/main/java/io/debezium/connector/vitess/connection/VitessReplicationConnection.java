/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnector;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.util.Strings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.vitess.client.Proto;
import io.vitess.client.grpc.StaticAuthCredentials;
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

    /**
     * Execute SQL statement via vtgate gRPC.
     * @param sqlStatement The SQL statement to be executed
     * @throws StatusRuntimeException if the connection is not valid, or SQL statement can not be successfully exected
     */
    public Vtgate.ExecuteResponse execute(String sqlStatement) {
        LOGGER.info("Executing sqlStament {}", sqlStatement);
        ManagedChannel channel = newChannel(config.getVtgateHost(), config.getVtgatePort(), config.getGrpcMaxInboundMessageSize());
        managedChannel.compareAndSet(null, channel);

        Vtgate.ExecuteRequest request = Vtgate.ExecuteRequest.newBuilder()
                .setQuery(Proto.bindQuery(sqlStatement, Collections.emptyMap()))
                .build();
        return newBlockingStub(channel).execute(request);
    }

    @Override
    public void startStreaming(
                               Vgtid vgtid, ReplicationMessageProcessor processor, AtomicReference<Throwable> error) {
        Objects.requireNonNull(vgtid);

        ManagedChannel channel = newChannel(config.getVtgateHost(), config.getVtgatePort(), config.getGrpcMaxInboundMessageSize());
        managedChannel.compareAndSet(null, channel);

        VitessGrpc.VitessStub stub = newStub(channel);

        Map<String, String> grpcHeaders = config.getGrpcHeaders();
        if (!grpcHeaders.isEmpty()) {
            LOGGER.info("Setting VStream gRPC headers: {}", grpcHeaders);
            Metadata metadata = new Metadata();
            for (Map.Entry<String, String> entry : grpcHeaders.entrySet()) {
                metadata.put(Metadata.Key.of(entry.getKey(), Metadata.ASCII_STRING_MARSHALLER), entry.getValue());
            }
            stub = MetadataUtils.attachHeaders(stub, metadata);
        }

        StreamObserver<Vtgate.VStreamResponse> responseObserver = new StreamObserver<Vtgate.VStreamResponse>() {
            private List<VEvent> bufferedEvents = new ArrayList<>();
            private Vgtid newVgtid;
            private boolean beginEventSeen;
            private boolean commitEventSeen;
            private int numOfRowEvents;
            private int numResponses;

            @Override
            public void onNext(Vtgate.VStreamResponse response) {
                LOGGER.debug("Received {} VEvents in the VStreamResponse:",
                        response.getEventsCount());
                boolean sendNow = false;
                for (VEvent event : response.getEventsList()) {
                    LOGGER.debug("VEvent: {}", event);
                    switch (event.getType()) {
                        case ROW:
                            numOfRowEvents++;
                            break;
                        case VGTID:
                            // We always use the latest VGTID if any.
                            if (newVgtid != null) {
                                if (newVgtid.getRawVgtid().getShardGtidsList().stream().findFirst().map(s -> s.getTablePKsCount() == 0).orElse(false)
                                        && event.getVgtid().getShardGtidsList().stream().findFirst().map(s -> 0 < s.getTablePKsCount()).orElse(false)) {
                                    LOGGER.info("Received more than one VGTID events during a copy operation and the previous one is {}. Using the latest: {}",
                                            newVgtid.toString(),
                                            event.getVgtid().toString());
                                }
                                else {
                                    LOGGER.warn("Received more than one VGTID events and the previous one is {}. Using the latest: {}",
                                            newVgtid.toString(),
                                            event.getVgtid().toString());
                                }
                            }
                            newVgtid = Vgtid.of(event.getVgtid());
                            break;
                        case BEGIN:
                            // We should only see BEGIN before seeing COMMIT.
                            if (commitEventSeen) {
                                String msg = "Received BEGIN event after receiving COMMIT event";
                                setError(msg);
                                return;
                            }
                            if (beginEventSeen) {
                                String msg = "Received duplicate BEGIN events";
                                // During a copy operation, we receive the duplicate event once when no record is copied.
                                String eventTypes = bufferedEvents.stream().map(VEvent::getType).map(Objects::toString).collect(Collectors.joining(", "));
                                if (eventTypes.equals("BEGIN, FIELD") || eventTypes.equals("BEGIN, FIELD, VGTID") || eventTypes.equals("COPY_COMPLETED, BEGIN, FIELD")) {
                                    msg += String.format(" during a copy operation. No harm to skip the buffered events. Buffered event types: %s",
                                            eventTypes);
                                    LOGGER.info(msg);
                                    reset();
                                }
                                else {
                                    setError(msg);
                                    return;
                                }
                            }
                            beginEventSeen = true;
                            break;
                        case COMMIT:
                            // We should only see COMMIT after seeing BEGIN.
                            if (!beginEventSeen) {
                                String msg = "Received COMMIT event before receiving BEGIN event";
                                setError(msg);
                                return;
                            }
                            if (commitEventSeen) {
                                String msg = "Received duplicate COMMIT events";
                                setError(msg);
                                return;
                            }
                            commitEventSeen = true;
                            messageDecoder.setCommitTimestamp(Instant.ofEpochSecond(event.getTimestamp()));
                            break;
                        case DDL:
                        case OTHER:
                            // If receiving DDL and OTHER, process them immediately to rotate vgtid in offset.
                            // For example, the response can be:
                            // [VGTID, DDL]. This is an DDL event.
                            // [VGTID, OTHER]. This is the first response if "current" is used as starting gtid.
                            sendNow = true;
                            break;
                    }
                    bufferedEvents.add(event);
                }

                numResponses++;

                // We only proceed when we receive a complete transaction after seeing both BEGIN and COMMIT events,
                // OR if sendNow flag is true (meaning we should process buffered events immediately).
                if ((!beginEventSeen || !commitEventSeen) && !sendNow) {
                    LOGGER.debug("Received partial transaction: number of responses so far is {}", numResponses);
                    return;
                }
                if (numResponses > 1) {
                    LOGGER.debug("Processing multi-response transaction: number of responses is {}", numResponses);
                }
                if (newVgtid == null) {
                    LOGGER.warn("Skipping because no vgtid is found in buffered event types: {}",
                            bufferedEvents.stream().map(VEvent::getType).map(Objects::toString).collect(Collectors.joining(", ")));
                    reset();
                    return;
                }

                // Send the buffered events that belong to the same transaction.
                try {
                    int rowEventSeen = 0;
                    for (int i = 0; i < bufferedEvents.size(); i++) {
                        Binlogdata.VEvent event = bufferedEvents.get(i);
                        if (event.getType() == Binlogdata.VEventType.ROW) {
                            rowEventSeen++;
                        }
                        boolean isLastRowEventOfTransaction = newVgtid != null && numOfRowEvents != 0 && rowEventSeen == numOfRowEvents;
                        messageDecoder.processMessage(bufferedEvents.get(i), processor, newVgtid, isLastRowEventOfTransaction);
                    }
                }
                catch (InterruptedException e) {
                    LOGGER.error("Message processing is interrupted", e);
                    // Only propagate the first error
                    error.compareAndSet(null, e);
                    Thread.currentThread().interrupt();
                }
                finally {
                    reset();
                }
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.error("VStream streaming onError. Status: {}", Status.fromThrowable(t), t);
                // Only propagate the first error
                error.compareAndSet(null, t);
                reset();
            }

            @Override
            public void onCompleted() {
                LOGGER.info("VStream streaming completed.");
                reset();
            }

            private void reset() {
                bufferedEvents.clear();
                newVgtid = null;
                beginEventSeen = false;
                commitEventSeen = false;
                numOfRowEvents = 0;
                numResponses = 0;
            }

            /**
             * Create and set an error for error handler and reset.
             */
            private void setError(String msg) {
                msg += String.format(". Buffered event types: %s",
                        bufferedEvents.stream().map(VEvent::getType).map(Objects::toString).collect(Collectors.joining(", ")));
                LOGGER.error(msg);
                error.compareAndSet(null, new DebeziumException(msg));
                reset();
            }
        };

        Vtgate.VStreamFlags vStreamFlags = Vtgate.VStreamFlags.newBuilder()
                .setStopOnReshard(config.getStopOnReshard())
                .build();
        // Add filtering for whitelist tables
        Binlogdata.Filter.Builder filterBuilder = Binlogdata.Filter.newBuilder();
        if (!Strings.isNullOrEmpty(config.tableIncludeList())) {
            final String keyspace = config.getKeyspace();
            final List<String> allTables = VitessConnector.getKeyspaceTables(config);
            List<String> includedTables = VitessConnector.getIncludedTables(config.getKeyspace(),
                    config.tableIncludeList(), allTables);
            for (String table : includedTables) {
                String sql = "select * from `" + table + "`";
                // See rule in: https://github.com/vitessio/vitess/blob/release-14.0/go/vt/vttablet/tabletserver/vstreamer/planbuilder.go#L316
                Binlogdata.Rule rule = Binlogdata.Rule.newBuilder().setMatch(table).setFilter(sql).build();
                LOGGER.info("Add vstream table filtering: {}", rule.getMatch());
                filterBuilder.addRules(rule);
            }
        }
        // Providing a vgtid MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-61 here will make VStream to
        // start receiving row-changes from MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-62
        Vtgate.VStreamRequest.Builder vstreamBuilder = Vtgate.VStreamRequest.newBuilder()
                .setVgtid(vgtid.getRawVgtid())
                .setTabletType(
                        toTopodataTabletType(VitessTabletType.valueOf(config.getTabletType())))
                .setFlags(vStreamFlags);
        if (filterBuilder.getRulesCount() > 0) {
            vstreamBuilder.setFilter(filterBuilder);
        }
        stub.vStream(
                vstreamBuilder.build(),
                responseObserver);
        LOGGER.info("Started VStream");
    }

    private VitessGrpc.VitessStub newStub(ManagedChannel channel) {
        VitessGrpc.VitessStub stub = VitessGrpc.newStub(channel);
        return withCredentials(stub);
    }

    private VitessGrpc.VitessBlockingStub newBlockingStub(ManagedChannel channel) {
        VitessGrpc.VitessBlockingStub stub = VitessGrpc.newBlockingStub(channel);
        return withCredentials(stub);
    }

    private <T extends AbstractStub<T>> T withCredentials(T stub) {
        if (config.getVtgateUsername() != null && config.getVtgatePassword() != null) {
            LOGGER.info("Use authenticated vtgate grpc.");
            stub = stub.withCallCredentials(new StaticAuthCredentials(config.getVtgateUsername(), config.getVtgatePassword()));
        }
        return stub;
    }

    private ManagedChannel newChannel(String vtgateHost, int vtgatePort, int maxInboundMessageSize) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(vtgateHost, vtgatePort)
                .usePlaintext()
                .maxInboundMessageSize(maxInboundMessageSize)
                .keepAliveTime(config.getKeepaliveInterval().toMillis(), TimeUnit.MILLISECONDS)
                .build();
        return channel;
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

    public static Vgtid buildVgtid(String keyspace, List<String> shards, List<String> gtids) {
        Binlogdata.VGtid.Builder builder = Binlogdata.VGtid.newBuilder();
        Vgtid vgtid;
        if (shards == null || shards.isEmpty()) {
            vgtid = Vgtid.of(builder.addShardGtids(
                    Binlogdata.ShardGtid.newBuilder()
                            .setKeyspace(keyspace)
                            .setGtid(Vgtid.CURRENT_GTID)
                            .build())
                    .build());
        }
        else {
            for (int i = 0; i < shards.size(); i++) {
                String shard = shards.get(i);
                String gtid = gtids.get(i);
                builder.addShardGtids(
                        Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(keyspace)
                                .setShard(shard)
                                .setGtid(gtid)
                                .build());
            }
            vgtid = Vgtid.of(builder.build());
        }
        LOGGER.info("Default VGTID '{}' for keyspace {}, shards: {}, gtids {}", vgtid, keyspace, shards, gtids);
        return vgtid;
    }

    /** Get latest replication position */
    public static Vgtid defaultVgtid(VitessConnectorConfig config) {
        Vgtid vgtid;
        if (config.offsetStoragePerTask()) {
            List<String> shards = config.getVitessTaskKeyShards();
            vgtid = config.getVitessTaskVgtid();
            LOGGER.info("VGTID '{}' is set for the keyspace: {} shards: {}",
                    vgtid, config.getKeyspace(), shards);
        }
        else {
            if (config.getShard() == null || config.getShard().isEmpty()) {
                // This case is not supported by the Vitess, so our workaround is to get all the shards from vtgate.
                if (config.getVgtid() == Vgtid.EMPTY_GTID) {
                    List<String> shards = VitessConnector.getVitessShards(config);
                    List<String> gtids = Collections.nCopies(shards.size(), config.getVgtid());
                    vgtid = buildVgtid(config.getKeyspace(), shards, gtids);
                }
                else {
                    vgtid = buildVgtid(config.getKeyspace(), Collections.emptyList(), Collections.emptyList());
                }
                LOGGER.info("Default VGTID '{}' is set to the current gtid of all shards from keyspace: {}",
                        vgtid, config.getKeyspace());
            }
            else {
                List<String> shards = config.getShard();
                String vgtidString = config.getVgtid();
                List<String> gtids;
                if (vgtidString == Vgtid.CURRENT_GTID ||
                        vgtidString == Vgtid.EMPTY_GTID) {
                    gtids = Collections.nCopies(shards.size(), vgtidString);
                    vgtid = buildVgtid(config.getKeyspace(), shards, gtids);
                }
                else {
                    vgtid = Vgtid.of(vgtidString);
                }
                LOGGER.info("VGTID '{}' is set to the GTID {} for keyspace: {} shard: {}",
                        vgtid, vgtidString, config.getKeyspace(), shards);
            }
        }
        return vgtid;
    }

    public String connectionString() {
        return String.format("vtgate gRPC connection %s:%s", config.getVtgateHost(), config.getVtgatePort());
    }

    public String username() {
        return config.getVtgateUsername();
    }

    private static Topodata.TabletType toTopodataTabletType(VitessTabletType tabletType) {
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
