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
 * Coordinates from the vitess database log to establish the relation between the change streamed
 * and the source log position. Maps to {@code source} field in {@code Envelope}.
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

    public void resetVgtid(Vgtid vgtid, Instant commitTime) {
        this.restartVgtid = vgtid;
        this.currentVgtid = vgtid;
        this.timestamp = commitTime;
    }

    /**
     * Rotate current and restart vgtid. Only rotate wen necessary.
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
