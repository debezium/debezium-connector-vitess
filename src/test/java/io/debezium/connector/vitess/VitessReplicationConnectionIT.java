/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.SchemaNameAdjuster;

import binlogdata.Binlogdata;

public class VitessReplicationConnectionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessReplicationConnectionIT.class);

    private static final String INSERT_STMT = "INSERT INTO t1 (int_col) VALUES (1);";
    private static final List<String> SETUP_TABLES_STMT = Arrays.asList(
            "DROP TABLE IF EXISTS t1;",
            "CREATE TABLE t1 (id BIGINT NOT NULL AUTO_INCREMENT, int_col INT, PRIMARY KEY (id));");

    protected long pollTimeoutInMs = SECONDS.toMillis(5);

    @Test
    public void shouldHaveVgtidInResponse() throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), VitessTopicSelector.defaultSelector(conf));

        // exercise SUT
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute(INSERT_STMT);

        AtomicReference<Throwable> error = new AtomicReference<>();
        try (VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema)) {
            Vgtid startingVgtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(conf.getKeyspace())
                                            .setShard(conf.getShard())
                                            .setGtid("current")
                                            .build())
                            .build());

            BlockingQueue<MessageAndVgtid> consumedMessages = new ArrayBlockingQueue<>(100);
            AtomicBoolean started = new AtomicBoolean(false);
            connection.startStreaming(
                    startingVgtid,
                    (message, vgtid, isLastRowEventOfTransaction) -> {
                        if (!started.get()) {
                            started.set(true);
                        }
                        consumedMessages.add(new MessageAndVgtid(message, vgtid));
                    },
                    error);
            // Since we are using the "current" as the starting position, there is a race here
            // if we execute INSERT_STMT before the vstream starts we will never receive the update
            // therefore, we wait until the stream is setup and then do the insertion
            Awaitility
                    .await()
                    .atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                    .until(started::get);
            int expectedNumOfMessages = 3;
            TestHelper.execute(INSERT_STMT);
            List<MessageAndVgtid> messages = await(
                    TestHelper.waitTimeForRecords(),
                    SECONDS,
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
            messages.forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard()));
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

        return new ArrayList<>(messages);
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
