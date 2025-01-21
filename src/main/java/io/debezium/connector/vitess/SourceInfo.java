/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Represents information about the source of a change event from Vitess.
 * This class is used to track the relationship between a change streamed and its position in
 * the source VStream. It includes fields that map to the {@code source} field in the {@code Envelope}.
 *
 * <p>
 * The {@link #currentVgtid} is the VGTID of the transaction currently being processed,
 * </p>
 *
 * <p>
 * The {@link #restartVgtid} is the VGTID of the last transaction that has been fully processed.
 * </p>
 *
 * <p>
 * When a VStream is resumed from transaction N, it receives transaction events starting at N+1 (exclusive start).
 * Thus, the {@link #restartVgtid} can only be updated once all events of the transaction have been successfully produced.
 * The {@link #currentVgtid} queues the VGTID of a transaction while it is being processed. The {@link #currentVgtid} is written to the
 * {@link #restartVgtid} once all events in the transaction have been produced.
 * </p>
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {
    public static final String VGTID_KEY = "vgtid";
    public static final String KEYSPACE_NAME_KEY = "keyspace";
    public static final String SHARD_KEY = "shard";

    private final String keyspace;

    private Vgtid currentVgtid;

    private TableId tableId;
    private Instant timestamp;
    // kafka offset topic stores restartVgtid, it is the previous commited transaction vgtid
    private Vgtid restartVgtid;
    private String shard;

    public SourceInfo(VitessConnectorConfig config) {
        super(config);
        this.keyspace = config.getKeyspace();
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected String database() {
        // Override DATABASE_NAME_KEY to empty string and in favor of KEYSPACE_NAME_KEY
        return null;
    }

    protected String keyspace() {
        return keyspace;
    }

    public String shard() {
        return shard;
    }

    public void setShard(String shard) {
        this.shard = shard;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Vgtid getCurrentVgtid() {
        return currentVgtid;
    }

    public Vgtid getRestartVgtid() {
        return restartVgtid;
    }

    /**
     * Reset the {@link #currentVgtid} and {@link #restartVgtid} to the provided vgtid
     * <p>Note: This should be only done if all events of that VGTID have been completely processed.</p>
     *
     * @param vgtid VGTID to set the current & restart VGTID to.
     * @param commitTime Commit time to update timestamp to
     */
    public void resetVgtid(Vgtid vgtid, Instant commitTime) {
        this.restartVgtid = vgtid;
        this.currentVgtid = vgtid;
        this.timestamp = commitTime;
    }

    /**
     * Rotates the new, current and restart VGTIDs when necessary.
     * Sets {@link #currentVgtid} to newVgtid. Sets {@link #restartVgtid} to {@link #currentVgtid}.
     * This should only be called after all events are produced successfully for {@link #currentVgtid}.
     *
     * @param newVgtid The new VGTID to set the current VGTID to.
     * @param commitTime Commit time to update timestamp to.
     */
    public void rotateVgtid(Vgtid newVgtid, Instant commitTime) {
        // Only rotate when necessary: when newVgtid is not the currentVgtid
        // Also, upon restart, the newVgtid could be null if the grpc response does not contain vgtid.
        if (!this.currentVgtid.equals(newVgtid)) {
            this.restartVgtid = this.currentVgtid;
            // keep using the same currentVgtid if the newVgtid is null
            if (newVgtid != null) {
                this.currentVgtid = newVgtid;
            }
            this.timestamp = commitTime;
        }
    }

    @Override
    public String toString() {
        return "SourceInfo{"
                + "tableId="
                + tableId
                + ", timestamp="
                + timestamp
                + ", currentVgtid="
                + currentVgtid
                + ", restartVgtid="
                + restartVgtid
                + '}';
    }
}
