/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.jdbc;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * Needed to maintain compatibility with RelationalSnapshotChangeEventSource
 *
 * TODO: Move all query-based interactions with Vitess onto this class.
 * Currently we do these with VitessReplicationConnection instead
 *
 * @author Thomas Thornton
 */
public class VitessConnection extends JdbcConnection {

    private static final String URL = "jdbc:mysql://${hostname}:${port}/${dbname}?maxAllowedPacket=512000";

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL);

    private static final String QUOTED_CHARACTER = "`";

    public VitessConnection(JdbcConfiguration config) {
        super(config, resolveConnectionFactory(), QUOTED_CHARACTER, QUOTED_CHARACTER);
    }

    private static ConnectionFactory resolveConnectionFactory() {
        return FACTORY;
    }
}
