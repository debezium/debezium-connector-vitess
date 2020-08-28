/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import io.debezium.connector.vitess.Vgtid;

/** Get the lastest VGTID position of a specific shard. */
public interface VgtidReader {

    /** The types of vitess tablet. */
    enum TabletType {
        /** Master mysql instance. */
        MASTER,

        /** Replica slave, can be promoted to master. */
        REPLICA,

        /** Read only slave, can not be promoted to master. */
        RDONLY;
    }

    /**
     * Get the latest VGTID position of a specific shard.
     *
     * @param keyspace
     * @param shard
     * @param tabletType
     * @return
     */
    Vgtid latestVgtid(String keyspace, String shard, TabletType tabletType);
}
