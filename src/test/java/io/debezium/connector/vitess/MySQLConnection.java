/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * A utility for integration test cases to connect the MySQL server (VTGate) running in the Docker
 * container created by this module's build.
 */
public class MySQLConnection extends JdbcConnection {

    /**
     * Obtain a connection instance to the named test database. Use the default database (a.k.a keyspace).
     *
     * @return the MySQLConnection instance; never null
     */
    public static MySQLConnection forTestDatabase() {
        return new MySQLConnection(
                JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                        .with("useSSL", false)
                        .build());
    }

    /**
     * Obtain a connection instance to the named test database.
     *
     * @param database the keyspace name
     * @return the MySQLConnection instance; never null
     */
    public static MySQLConnection forTestDatabase(String database) {
        return new MySQLConnection(
                JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                        .with(JdbcConfiguration.DATABASE, database)
                        .with("useSSL", false)
                        .build());
    }

    protected static void addDefaults(Configuration.Builder builder) {
        builder
                .withDefault(JdbcConfiguration.DATABASE, TestHelper.TEST_UNSHARDED_KEYSPACE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 15306)
                .withDefault(JdbcConfiguration.USER, "")
                .withDefault(JdbcConfiguration.PASSWORD, "");
    }

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("jdbc:mysql://${hostname}:${port}/${dbname}");

    /**
     * Create a new instance with the given configuration and connection factory.
     *
     * @param config the configuration; may not be null
     */
    public MySQLConnection(Configuration config) {
        super(config, FACTORY, null, MySQLConnection::addDefaults, "`", "`");
    }
}
