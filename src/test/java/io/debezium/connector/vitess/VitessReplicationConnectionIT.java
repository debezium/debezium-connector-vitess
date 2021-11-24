/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.fest.assertions.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.util.SchemaNameAdjuster;

import binlogdata.Binlogdata;

public class VitessReplicationConnectionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessReplicationConnectionIT.class);

    protected long pollTimeoutInMs = SECONDS.toMillis(5);

    @Test
    public void shouldHaveVgtidInResponse() throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), VitessTopicSelector.defaultSelector(conf));

        // exercise SUT
        TestHelper.execute(TestHelper.SETUP_TABLES_STMT);
        TestHelper.execute(TestHelper.INSERT_STMT);

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

            BlockingQueue<TestHelper.MessageAndVgtid> consumedMessages = new ArrayBlockingQueue<>(100);
            AtomicBoolean started = new AtomicBoolean(false);
            connection.startStreaming(
                    startingVgtid,
                    (message, vgtid, isLastRowEventOfTransaction) -> {
                        if (!started.get()) {
                            started.set(true);
                        }
                        consumedMessages.add(new TestHelper.MessageAndVgtid(message, vgtid));
                    },
                    error);
            // Since we are using the "current" as the starting position, there is a race here
            // if we execute INSERT_STMT before the vstream starts we will never receive the update
            // therefore, we wait until the stream is setup and then do the insertion
            Awaitility
                    .await()
                    .atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                    .until(started::get);
            consumedMessages.clear();
            int expectedNumOfMessages = 3;
            TestHelper.execute(TestHelper.INSERT_STMT);
            List<TestHelper.MessageAndVgtid> messages = TestHelper.awaitMessages(
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
            assertThat(messages.get(0).getMessage().getOperation().name()).isEqualTo("BEGIN");
            assertThat(messages.get(1).getMessage().getOperation().name()).isEqualTo("INSERT");
            assertThat(messages.get(2).getMessage().getOperation().name()).isEqualTo("COMMIT");
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
}
