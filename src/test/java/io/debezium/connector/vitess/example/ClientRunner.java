/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.example;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Vitess gRPC client runner for debugging purpose.
 * <p/>
 * args: keyspace shards gtid_index, For example:
 * <p/>
 * mvn exec:java -Dexec.mainClass="io.debezium.connector.vitess.example.ClientRunner" -Dexec.args="commerce - 7 localhost"
 * <p/>
 * mvn exec:java -Dexec.mainClass="io.debezium.connector.vitess.example.ClientRunner" -Dexec.args="commerce 0 8 localhost"
 * <p/>
 * mvn exec:java -Dexec.mainClass="io.debezium.connector.vitess.example.ClientRunner" -Dexec.args="customer -80,80- 7 localhost"
 */
public class ClientRunner {
    private static final java.util.logging.Logger LOGGER = Logger.getLogger(ClientRunner.class.getName());

    public static void main(String[] args) throws InterruptedException {
        String keyspace = args[0];
        List<String> shards = Arrays.asList(args[1].split(","));
        int gtidIdx = Integer.valueOf(args[2]);
        String host = args[3];
        LOGGER.info("keyspace: " + keyspace);
        LOGGER.info("shards: " + shards);
        LOGGER.info("gtidIdx: " + gtidIdx);
        LOGGER.info("host: " + host);

        AbstractVStreamClient client = new BlockingVStreamClient(keyspace, shards, gtidIdx, host);
        // AbstractVStreamClient client = new BlockingDeadlineVStreamClient(5);
        // AbstractVStreamClient client = new AsyncVStreamClient(60);
        client.startStreaming();
    }
}
