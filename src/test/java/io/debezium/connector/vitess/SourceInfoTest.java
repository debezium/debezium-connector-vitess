/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.SnapshotRecord;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

public class SourceInfoTest {
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_SHARD = "-80";
    private static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1513";
    private static final String TEST_SHARD2 = "80-";
    private static final String TEST_GTID2 = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000b:1-1513";
    private static final String VGTID_JSON = String.format(
            "[" +
                    "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\"}," +
                    "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\"}" +
                    "]",
            TEST_KEYSPACE,
            TEST_SHARD,
            TEST_GTID,
            TEST_KEYSPACE,
            TEST_SHARD2,
            TEST_GTID2);

    private SourceInfo source;

    @Before
    public void beforeEach() {
        final VitessConnectorConfig connectorConfig = new VitessConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "server_foo")
                        .with(VitessConnectorConfig.KEYSPACE, TEST_KEYSPACE)
                        .with(VitessConnectorConfig.SHARD, AnonymousValue.getString())
                        .with(VitessConnectorConfig.VTGATE_HOST, AnonymousValue.getString())
                        .with(VitessConnectorConfig.VTGATE_PORT, AnonymousValue.getInt())
                        .build());
        source = new SourceInfo(connectorConfig);
        source.resetVgtid(
                Vgtid.of(
                        Collect.arrayListOf(
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2))),
                Instant.ofEpochMilli(1000));
        source.setTableId(new TableId("c", "s", "t"));
        source.setShard(TEST_SHARD);
        source.setSnapshot(SnapshotRecord.FALSE);
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY))
                .isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY))
                .isEqualTo(Module.name());
    }

    @Test
    public void serverNameIsPresent() {
        assertThat(source.struct().getString(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server_foo");
    }

    @Test
    public void vgtidKeyspaceIsPresent() {
        assertThat(source.struct().getString(SourceInfo.VGTID_KEY)).isEqualTo(VGTID_JSON);
    }

    @Test
    public void shardIsPresent() {
        assertThat(source.struct().getString(SourceInfo.SHARD_KEY)).isEqualTo(TEST_SHARD);
    }

    @Test
    public void snapshotIsNotPresent() {
        // If SnapshotRecord.FALSE, no snapshot is present
        assertThat(source.struct().getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    @Test
    public void timestampIsPresent() {
        assertThat(source.struct().getInt64(SourceInfo.TIMESTAMP_KEY)).isEqualTo(1000);
    }

    @Test
    public void databaseIsEmpty() {
        assertThat(source.struct().getString(SourceInfo.DATABASE_NAME_KEY)).isEmpty();
    }

    @Test
    public void tableIsPresent() {
        assertThat(source.struct().getString(SourceInfo.TABLE_NAME_KEY)).isEqualTo("t");
    }

    @Test
    public void keyspaceIsPresent() {
        assertThat(source.struct().getString(SourceInfo.KEYSPACE_NAME_KEY)).isEqualTo(TEST_KEYSPACE);
    }

    @Test
    public void schemaIsCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.vitess.Source")
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("keyspace", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("shard", Schema.STRING_SCHEMA)
                .field("vgtid", Schema.STRING_SCHEMA)
                .build();

        assertThat(source.struct().schema()).isEqualTo(schema);
    }
}
