/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.relational.TableId;

public class VitessSourceInfoStructMakerTest {
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_SHARD = "0";
    private static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1514";

    @Test
    public void shouldGetCorrectSourceInfoSchema() {
        VitessSourceInfoStructMaker structMaker = new VitessSourceInfoStructMaker(
                "test_connector",
                "test_version",
                new VitessConnectorConfig(TestHelper.defaultConfig().build()));

        assertThat(structMaker.schema().field(SourceInfo.SCHEMA_NAME_KEY).schema())
                .isEqualTo(Schema.STRING_SCHEMA);
        assertThat(structMaker.schema().field(SourceInfo.TABLE_NAME_KEY).schema())
                .isEqualTo(Schema.STRING_SCHEMA);
        assertThat(structMaker.schema().field(SourceInfo.VGTID_KEYSPACE).schema())
                .isEqualTo(Schema.STRING_SCHEMA);
        assertThat(structMaker.schema().field(SourceInfo.VGTID_SHARD).schema())
                .isEqualTo(Schema.STRING_SCHEMA);
        assertThat(structMaker.schema().field(SourceInfo.VGTID_GTID).schema())
                .isEqualTo(Schema.STRING_SCHEMA);
        assertThat(structMaker.schema()).isNotNull();
    }

    @Test
    public void shouldGetCorrectSourceInfoStruct() {
        // setup fixture
        String schemaName = "test_schema";
        String tableName = "test_table";
        SourceInfo sourceInfo = new SourceInfo(new VitessConnectorConfig(TestHelper.defaultConfig().build()));
        sourceInfo.initialVgtid(
                Vgtid.of(TEST_KEYSPACE, TEST_SHARD, TEST_GTID), AnonymousValue.getInstant());
        sourceInfo.rotateVgtid(
                Vgtid.of(TEST_KEYSPACE, TEST_SHARD, TEST_GTID), AnonymousValue.getInstant());
        sourceInfo.startRowEvent(AnonymousValue.getInstant(), new TableId(null, schemaName, tableName));

        // exercise SUT
        Struct struct = new VitessSourceInfoStructMaker(
                "test_connector",
                "test_version",
                new VitessConnectorConfig(TestHelper.defaultConfig().build()))
                        .struct(sourceInfo);

        // verify outcome
        assertThat(struct.getString(SourceInfo.SCHEMA_NAME_KEY)).isEqualTo(schemaName);
        assertThat(struct.getString(SourceInfo.TABLE_NAME_KEY)).isEqualTo(tableName);
        assertThat(struct.getString(SourceInfo.VGTID_KEYSPACE)).isEqualTo(TEST_KEYSPACE);
        assertThat(struct.getString(SourceInfo.VGTID_SHARD)).isEqualTo(TEST_SHARD);
        assertThat(struct.getString(SourceInfo.VGTID_GTID)).isEqualTo(TEST_GTID);
    }
}
