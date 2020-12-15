/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.connector.vitess.VitessTopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.SchemaNameAdjuster;

public class VitessReplicationConnectionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessReplicationConnectionIT.class);

    private static final String INSERT_STMT = "INSERT INTO t1 (int_col) VALUES (1);";
    private static final List<String> SETUP_TABLES_STMT = Arrays.asList(
            "DROP TABLE IF EXISTS t1;",
            "CREATE TABLE t1 (id BIGINT NOT NULL AUTO_INCREMENT, int_col INT, PRIMARY KEY (id));");

    protected long pollTimeoutInMs = TimeUnit.SECONDS.toMillis(5);

    @Test
    public void shouldHaveVgtidInResponse() throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(LOGGER), VitessTopicSelector.defaultSelector(conf));

        // exercise SUT
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        AtomicReference<Throwable> error = new AtomicReference<>();
        try (VtctldConnection vtctldConnection = VtctldConnection.of(
                conf.getVtctldHost(), conf.getVtctldPort(), conf.getVtctldUsername(), conf.getVtctldPassword());
                VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema)) {

            Vgtid startingVgtid = vtctldConnection.latestVgtid(
                    conf.getKeyspace(),
                    conf.getShard(),
                    VtctldConnection.TabletType.valueOf(conf.getTabletType()));

            BlockingQueue<MessageAndVgtid> consumedMessages = new ArrayBlockingQueue<>(100);
            TestHelper.execute(String.format(INSERT_STMT));
            connection.startStreaming(
                    startingVgtid,
                    (message, vgtid, isLastRowEventOfTransaction) -> {
                        consumedMessages.add(new MessageAndVgtid(message, vgtid));
                    },
                    error);

            int expectedNumOfMessages = 3;
            List<MessageAndVgtid> messages = await(
                    TestHelper.waitTimeForRecords(),
                    TimeUnit.SECONDS,
                    expectedNumOfMessages,
                    () -> {
                        try {
                            return consumedMessages.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
                        }
                        catch (InterruptedException e) {
                            return null;
                        }
                    });

            // verify outcome
            messages.stream()
                    .forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard()));
        }
        finally {
            if (error.get() != null) {
                LOGGER.error("Error during streaming", error.get());
            }
        }
    }

    private void assertValidVgtid(Vgtid vgtid, String expectedKeyspace, String expectedShard) {
        assertThat(vgtid.getShardGtids()).hasSize(1);
        assertThat(vgtid.getShardGtids().iterator().next().getKeyspace()).isEqualTo(expectedKeyspace);
        assertThat(vgtid.getShardGtids().iterator().next().getShard()).isEqualTo(expectedShard);
        String gtid = vgtid.getShardGtids().iterator().next().getGtid();
        assertThat(gtid.startsWith("MySQL") || gtid.startsWith("MariaDB")).isTrue();
    }

    private static class MessageAndVgtid {
        ReplicationMessage message;
        Vgtid vgtid;

        public MessageAndVgtid(ReplicationMessage message, Vgtid vgtid) {
            this.message = message;
            this.vgtid = vgtid;
        }

        public ReplicationMessage getMessage() {
            return message;
        }

        public Vgtid getVgtid() {
            return vgtid;
        }
    }

    protected List<MessageAndVgtid> await(
                                          long timeout, TimeUnit unit, int expectedNumOfMessages, Supplier<MessageAndVgtid> supplier) {
        final ElapsedTimeStrategy timer = ElapsedTimeStrategy.constant(Clock.SYSTEM, unit.toMillis(timeout));
        timer.hasElapsed();

        ConcurrentLinkedQueue<MessageAndVgtid> messages = new ConcurrentLinkedQueue<>();

        while (!timer.hasElapsed()) {
            final MessageAndVgtid r = supplier.get();
            if (r != null) {
                accept(r, expectedNumOfMessages, messages);
                if (messages.size() == expectedNumOfMessages) {
                    break;
                }
            }
        }
        if (messages.size() != expectedNumOfMessages) {
            fail(
                    "Consumer is still expecting "
                            + (expectedNumOfMessages - messages.size())
                            + " records, as it received only "
                            + messages.size());
        }

        return messages.stream().collect(Collectors.toList());
    }

    private void accept(
                        MessageAndVgtid message,
                        int expectedNumOfMessages,
                        ConcurrentLinkedQueue<MessageAndVgtid> messages) {
        if (messages.size() >= expectedNumOfMessages) {
            fail("received more events than expected");
        }
        else {
            messages.add(message);
        }
    }
}
