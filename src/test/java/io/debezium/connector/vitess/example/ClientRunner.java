/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Vitess gRPC client runner for debugging purpose.
 * <p/>
 * args: keyspace shards gtid_index, For example:
 * <p/>
 * mvn exec:java -Dexec.mainClass="io.debezium.connector.vitess.example.ClientRunner" -Dexec.args="localhost commerce - 7"
 * <p/>
 * mvn exec:java -Dexec.mainClass="io.debezium.connector.vitess.example.ClientRunner" -Dexec.args="localhost commerce 0 8"
 * <p/>
 * mvn exec:java -Dexec.mainClass="io.debezium.connector.vitess.example.ClientRunner" -Dexec.args="localhost customer -80,80- 7"
 * <p/>
 * mvn exec:java -Dexec.mainClass="io.debezium.connector.vitess.example.ClientRunner" -Dexec.args="localhost commerce"
 */
public class ClientRunner {
    private static final java.util.logging.Logger LOGGER = Logger.getLogger(ClientRunner.class.getName());

    public static void main(String[] args) throws InterruptedException {
        String host = args[0];
        String keyspace = args[1];
        LOGGER.info("host: " + host);
        LOGGER.info("keyspace: " + keyspace);

        List<String> shards = new ArrayList<>();
        int gtidIdx = -1;
        if (args.length > 2) {
            shards = Arrays.asList(args[2].split(","));
            gtidIdx = Integer.valueOf(args[3]);
            LOGGER.info("shards: " + shards);
            LOGGER.info("gtidIdx: " + gtidIdx);
        }

        AbstractVStreamClient client = new BlockingVStreamClient(keyspace, shards, gtidIdx, host);
        // AbstractVStreamClient client = new BlockingDeadlineVStreamClient(5);
        // AbstractVStreamClient client = new AsyncVStreamClient(60);
        client.startStreaming();
    }
}
