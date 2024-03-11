/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_UNSHARDED_KEYSPACE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

import binlogdata.Binlogdata;
import ch.qos.logback.classic.Level;

public class VitessReplicationConnectionIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessReplicationConnectionIT.class);

    protected long pollTimeoutInMs = SECONDS.toMillis(5);

    @Before
    public void beforeEach() {
        TestHelper.execute(TestHelper.SETUP_TABLES_STMT);
        TestHelper.execute(TestHelper.INSERT_STMT);
    }

    @Test
    public void shouldErrorOutWhenSkipEnabled() throws Exception {
        // setup fixture
        final LogInterceptor logInterceptor = new LogInterceptor(VitessReplicationConnection.class);
        logInterceptor.setLoggerLevel(VitessReplicationConnection.class, Level.DEBUG);
        final VitessConnectorConfig conf = new VitessConnectorConfig(
                TestHelper.defaultConfig(false, false, 1, -1, -1,
                        null, VitessConnectorConfig.SnapshotMode.NEVER, TestHelper.TEST_SHARD,
                        "1", "skip").build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema);
        Vgtid startingVgtid = Vgtid.of(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(
                                Binlogdata.ShardGtid.newBuilder()
                                        .setKeyspace(conf.getKeyspace())
                                        .setShard(conf.getShard().get(0))
                                        .setGtid(Vgtid.CURRENT_GTID)
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
                .until(() -> error.get() != null);
        assertThat(error.get()).isNotNull();
    }

    @Test
    public void shouldErrorOutWhenWarnEnabled() throws Exception {
        // setup fixture
        final LogInterceptor logInterceptor = new LogInterceptor(VitessReplicationConnection.class);
        final VitessConnectorConfig conf = new VitessConnectorConfig(
                TestHelper.defaultConfig(false, false, 1, -1, -1,
                        null, VitessConnectorConfig.SnapshotMode.NEVER, TestHelper.TEST_SHARD,
                        "1", "warn").build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema);
        Vgtid startingVgtid = Vgtid.of(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(
                                Binlogdata.ShardGtid.newBuilder()
                                        .setKeyspace(conf.getKeyspace())
                                        .setShard(conf.getShard().get(0))
                                        .setGtid(Vgtid.CURRENT_GTID)
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
                .until(() -> error.get() != null);
        assertThat(error.get()).isNotNull();
    }

    @Test
    public void shouldFailWhenFailEnabled() throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(
                TestHelper.defaultConfig(false, false, 1, -1, -1,
                        null, VitessConnectorConfig.SnapshotMode.NEVER, TestHelper.TEST_SHARD,
                        "1", "fail").build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema);
        Vgtid startingVgtid = Vgtid.of(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(
                                Binlogdata.ShardGtid.newBuilder()
                                        .setKeyspace(conf.getKeyspace())
                                        .setShard(conf.getShard().get(0))
                                        .setGtid(Vgtid.CURRENT_GTID)
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
                .until(() -> error.get() != null);
        assertThat(error.get()).isNotNull();
    }

    @Test
    public void shouldFailWhenErrorProcessingModeIsNotSet() throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(
                TestHelper.defaultConfig(false, false, 1, -1, -1,
                        null, VitessConnectorConfig.SnapshotMode.NEVER, TestHelper.TEST_SHARD,
                        "1", null).build());
        conf.getEventProcessingFailureHandlingMode();
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema);
        Vgtid startingVgtid = Vgtid.of(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(
                                Binlogdata.ShardGtid.newBuilder()
                                        .setKeyspace(conf.getKeyspace())
                                        .setShard(conf.getShard().get(0))
                                        .setGtid(Vgtid.CURRENT_GTID)
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
                .until(() -> error.get() != null);
        assertThat(error.get()).isNotNull();
    }

    @Test
    public void shouldHaveVgtidInResponse() throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        try (VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema)) {
            Vgtid startingVgtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(conf.getKeyspace())
                                            .setShard(conf.getShard().get(0))
                                            .setGtid(Vgtid.CURRENT_GTID)
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
            consumedMessages.clear();
            int expectedNumOfMessages = 3;
            TestHelper.execute(TestHelper.INSERT_STMT);
            List<MessageAndVgtid> messages = awaitMessages(
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
            messages.forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard().get(0)));
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

    @Test
    public void shouldSendCommitTimestamp() throws Exception {
        // setup fixture
        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        try (VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema)) {
            Vgtid startingVgtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(conf.getKeyspace())
                                            .setShard(conf.getShard().get(0))
                                            .setGtid(Vgtid.CURRENT_GTID)
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
            consumedMessages.clear();
            int expectedNumOfMessages = 3;
            MySQLConnection testConnection = MySQLConnection.forTestDatabase(TEST_UNSHARDED_KEYSPACE);
            testConnection.setAutoCommit(false);
            testConnection.executeWithoutCommitting("BEGIN");
            Thread.sleep(1000);
            testConnection.executeWithoutCommitting(TestHelper.INSERT_STMT);
            Thread.sleep(1000);
            testConnection.executeWithoutCommitting("COMMIT");
            List<MessageAndVgtid> messages = awaitMessages(
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

            messages.forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard().get(0)));
            assertThat(messages.get(0).getMessage().getOperation().name()).isEqualTo("BEGIN");
            Instant beginTime = messages.get(0).getMessage().getCommitTime();
            assertThat(messages.get(1).getMessage().getOperation().name()).isEqualTo("INSERT");
            Instant insertTime = messages.get(1).getMessage().getCommitTime();
            assertThat(messages.get(2).getMessage().getOperation().name()).isEqualTo("COMMIT");
            Instant commitTime = messages.get(2).getMessage().getCommitTime();
            assertThat(beginTime).isNotEqualTo(commitTime);
            assertThat(insertTime).isEqualTo(commitTime);

            testConnection.executeWithoutCommitting("BEGIN");
            Thread.sleep(1000);
            testConnection.executeWithoutCommitting(TestHelper.INSERT_STMT);
            Thread.sleep(1000);
            testConnection.executeWithoutCommitting("COMMIT");
            List<MessageAndVgtid> messages2 = awaitMessages(
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

            messages2.forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard().get(0)));
            assertThat(messages2.get(0).getMessage().getOperation().name()).isEqualTo("BEGIN");
            Instant beginTime2 = messages2.get(0).getMessage().getCommitTime();
            assertThat(messages2.get(1).getMessage().getOperation().name()).isEqualTo("INSERT");
            Instant insertTime2 = messages2.get(1).getMessage().getCommitTime();
            assertThat(messages2.get(2).getMessage().getOperation().name()).isEqualTo("COMMIT");
            Instant commitTime2 = messages2.get(2).getMessage().getCommitTime();
            assertThat(beginTime2).isNotEqualTo(commitTime2);
            assertThat(insertTime2).isEqualTo(commitTime2);

            // Verify we use the commit time of the second transaction
            assertThat(commitTime).isNotEqualTo(commitTime2);
        }
        finally {
            if (error.get() != null) {
                LOGGER.error("Error during streaming", error.get());
            }
        }
    }

    @Test
    public void shouldCopyAndReplicate() throws Exception {
        // setup fixture
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + "t1";

        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, tableInclude).build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        try (VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema)) {
            Vgtid startingVgtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(conf.getKeyspace())
                                            .setShard(conf.getShard().get(0))
                                            .setGtid("")
                                            .build())
                            .build());

            BlockingQueue<MessageAndVgtid> consumedMessages = new ArrayBlockingQueue<>(100);
            connection.startStreaming(
                    startingVgtid,
                    (message, vgtid, isLastRowEventOfTransaction) -> {
                        consumedMessages.add(new MessageAndVgtid(message, vgtid));
                    },
                    error);

            int expectedNumOfMessages = 3;
            List<MessageAndVgtid> messages = awaitMessages(
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

            // verify outcome from the copy operation
            messages.forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard().get(0)));
            assertThat(messages.get(0).getMessage().getOperation().name()).isEqualTo("BEGIN");
            assertThat(messages.get(1).getMessage().getOperation().name()).isEqualTo("INSERT");
            assertThat(messages.get(2).getMessage().getOperation().name()).isEqualTo("COMMIT");

            consumedMessages.clear();

            TestHelper.execute(TestHelper.INSERT_STMT);
            messages = awaitMessages(
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

            // verify outcome from the replicate operation
            messages.forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard().get(0)));
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

    @Test
    @FixFor("DBZ-4353")
    public void shouldReturnUpdatedSchemaWithOnlineDdl() throws Exception {
        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));
        AtomicReference<Throwable> error = new AtomicReference<>();
        try (VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema)) {
            Vgtid startingVgtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(conf.getKeyspace())
                                            .setShard(conf.getShard().get(0))
                                            .setGtid(Vgtid.CURRENT_GTID)
                                            .build())
                            .build());

            BlockingQueue<MessageAndVgtid> consumedMessages = new ArrayBlockingQueue<>(200);
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
            Awaitility
                    .await()
                    .atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                    .until(started::get);
            consumedMessages.clear();
            // OnlineDDL is async, hence we issue the command and wait until it finishes
            String ddlId = TestHelper.applyOnlineDdl("ALTER TABLE t1 ADD COLUMN name varchar(64)", TestHelper.TEST_UNSHARDED_KEYSPACE);
            Awaitility
                    .await()
                    .atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> TestHelper.checkOnlineDDL(TestHelper.TEST_UNSHARDED_KEYSPACE, ddlId));
            TestHelper.execute("UPDATE t1 SET name='abc' WHERE id=1");
            MessageAndVgtid message = awaitOperation(
                    TestHelper.waitTimeForRecords(),
                    SECONDS,
                    "UPDATE",
                    () -> {
                        try {
                            return consumedMessages.poll(SECONDS.toMillis(5), TimeUnit.MILLISECONDS);
                        }
                        catch (InterruptedException e) {
                            return null;
                        }
                    });
            ReplicationMessage dml = message.getMessage();
            assertThat(dml.getOperation().name()).isEqualTo("UPDATE");
            // After the schema migration, both getOldTupleList and getNewTupleList has 3 fields
            // the old one has null value whereas the new one has value set
            assertThat(dml.getOldTupleList().get(0).getName()).isEqualTo("id");
            assertThat(dml.getOldTupleList().get(1).getName()).isEqualTo("int_col");
            assertThat(dml.getOldTupleList().get(2).getName()).isEqualTo("name");
            assertThat(dml.getOldTupleList().get(2).getValue(false)).isNull();
            assertThat(dml.getNewTupleList().get(0).getName()).isEqualTo("id");
            assertThat(dml.getNewTupleList().get(1).getName()).isEqualTo("int_col");
            assertThat(dml.getNewTupleList().get(2).getName()).isEqualTo("name");
            assertThat(dml.getNewTupleList().get(2).getValue(false)).isEqualTo("abc");
        }
        finally {
            if (error.get() != null) {
                LOGGER.error("Error during streaming", error.get());
            }
        }
    }

    @Test
    public void shouldNotFailWhenTableNameIsReservedKeyword() throws Exception {

        // setup fixture
        TestHelper.execute(Arrays.asList(
                "DROP TABLE IF EXISTS `schemas`;",
                "CREATE TABLE `schemas` (id BIGINT NOT NULL AUTO_INCREMENT, int_col INT, PRIMARY KEY (id));"));
        TestHelper.execute("INSERT INTO `schemas` (int_col) VALUES (1);");

        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + "schemas";

        final VitessConnectorConfig conf = new VitessConnectorConfig(TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, tableInclude).build());
        final VitessDatabaseSchema vitessDatabaseSchema = new VitessDatabaseSchema(
                conf, SchemaNameAdjuster.create(), (TopicNamingStrategy) DefaultTopicNamingStrategy.create(conf));

        AtomicReference<Throwable> error = new AtomicReference<>();
        try (VitessReplicationConnection connection = new VitessReplicationConnection(conf, vitessDatabaseSchema)) {
            Vgtid startingVgtid = Vgtid.of(
                    Binlogdata.VGtid.newBuilder()
                            .addShardGtids(
                                    Binlogdata.ShardGtid.newBuilder()
                                            .setKeyspace(conf.getKeyspace())
                                            .setShard(conf.getShard().get(0))
                                            .setGtid("")
                                            .build())
                            .build());

            BlockingQueue<MessageAndVgtid> consumedMessages = new ArrayBlockingQueue<>(100);
            connection.startStreaming(
                    startingVgtid,
                    (message, vgtid, isLastRowEventOfTransaction) -> {
                        consumedMessages.add(new MessageAndVgtid(message, vgtid));
                    },
                    error);

            int expectedNumOfMessages = 3;
            List<MessageAndVgtid> messages = awaitMessages(
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

            // verify outcome from the copy operation
            messages.forEach(m -> assertValidVgtid(m.getVgtid(), conf.getKeyspace(), conf.getShard().get(0)));
            assertThat(messages.get(0).getMessage().getOperation().name()).isEqualTo("BEGIN");
            assertThat(messages.get(1).getMessage().getOperation().name()).isEqualTo("INSERT");
            assertThat(messages.get(2).getMessage().getOperation().name()).isEqualTo("COMMIT");

            assertThat(error.get()).isNull();
        }
    }

    private void assertValidVgtid(Vgtid vgtid, String expectedKeyspace, String expectedShard) {
        assertThat(vgtid.getShardGtids()).hasSize(1);
        assertThat(vgtid.getShardGtids().iterator().next().getKeyspace()).isEqualTo(expectedKeyspace);
        assertThat(vgtid.getShardGtids().iterator().next().getShard()).isEqualTo(expectedShard);
        String gtid = vgtid.getShardGtids().iterator().next().getGtid();
        assertThat(gtid.startsWith("MySQL") || gtid.startsWith("MariaDB")).isTrue();
    }

    private static List<MessageAndVgtid> awaitMessages(
                                                       long timeout, TimeUnit unit, int expectedNumOfMessages, Supplier<MessageAndVgtid> supplier) {
        ConcurrentLinkedQueue<MessageAndVgtid> messages = new ConcurrentLinkedQueue<>();
        Awaitility
                .await()
                .atMost(Duration.ofMillis(unit.toMillis(timeout)))
                .until(() -> {
                    final MessageAndVgtid r = supplier.get();
                    if (r != null) {
                        if (messages.size() >= expectedNumOfMessages) {
                            fail("received more events than expected");
                        }
                        else {
                            messages.add(r);
                        }
                        return messages.size() == expectedNumOfMessages;
                    }
                    return false;
                });
        if (messages.size() != expectedNumOfMessages) {
            fail(
                    "Consumer is still expecting "
                            + (expectedNumOfMessages - messages.size())
                            + " records, as it received only "
                            + messages.size());
        }
        return new ArrayList<>(messages);
    }

    private static MessageAndVgtid awaitOperation(
                                                  long timeout, TimeUnit unit, String expectedOperation, Supplier<MessageAndVgtid> supplier) {
        AtomicReference<MessageAndVgtid> result = new AtomicReference<>();
        Awaitility
                .await()
                .atMost(Duration.ofMillis(unit.toMillis(timeout)))
                .until(() -> {
                    final MessageAndVgtid r = supplier.get();
                    if (r != null && r.getMessage().getOperation().name().equals(expectedOperation)) {
                        result.set(r);
                        return true;
                    }
                    return false;
                });
        return result.get();
    }

    private static class MessageAndVgtid {
        ReplicationMessage message;
        Vgtid vgtid;

        MessageAndVgtid(ReplicationMessage message, Vgtid vgtid) {
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
}
