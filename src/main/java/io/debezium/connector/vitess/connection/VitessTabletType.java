/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

/**
 * The types of vitess tablet.
 */
public enum VitessTabletType {
    // Master mysql instance.
    MASTER,

    // Replica slave, can be promoted to master.
    REPLICA,

    // Read only slave, can not be promoted to master.
    RDONLY;
}
