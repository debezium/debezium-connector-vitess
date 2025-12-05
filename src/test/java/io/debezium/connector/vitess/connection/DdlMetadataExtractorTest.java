/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.schema.SchemaChangeEvent;

/**
 * @author Thomas Thornton
 */
public class DdlMetadataExtractorTest {

    private static String expectedTableName = VitessDatabaseSchema.buildTableId(
            TestHelper.TEST_SHARD, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE).toDoubleQuotedString();

    @Test
    public void shouldGetAlterType() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("ALTER TABLE %s ADD COLUMN bar", TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldGetCreateType() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("CREATE    TABLE %s", TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.CREATE);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldGetTruncateType() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("TRUNCATE    TABLE %s", TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.TRUNCATE);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldGetTable() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("TRUNCATE    TABLE %s", TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldGetDropType() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("DROP TABLE %s", TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.DROP);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldGetRenameType() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("RENAME TABLE %s TO %s_suffix", TestHelper.TEST_TABLE, TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldParseStatementWithComments() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("rename /* gh-ost */ table `keyspace`.`%s` to `keyspace`.`_table1_del`, `keyspace`.`_table_gho` to `keyspace`.`table`",
                        TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldParseStatementWithSingleLineComment() {
        DdlMessage ddlMessage = new DdlMessage(null, null,
                String.format("create \n # gh-ost \n table `keyspace`.`%s`;", TestHelper.TEST_TABLE),
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.CREATE);
        assertThat(extractor.getTable()).isEqualTo(expectedTableName);
    }

    @Test
    public void shouldGracefullyHandleUnparseableStatement() {
        DdlMessage ddlMessage = new DdlMessage(null, null, "UNPARSEABLE command to a table",
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        final LogInterceptor logInterceptor = new LogInterceptor(DdlMetadataExtractor.class);
        String unknownTable = "Unknown table";
        String unknownType = "Unknown schema change event type";
        assertThat(extractor.getTable()).isEqualTo("\"0\".\"test_unsharded_keyspace\".\"<UNKNOWN>\"");
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(logInterceptor.containsWarnMessage(unknownTable)).isTrue();
        assertThat(logInterceptor.containsWarnMessage(unknownType)).isTrue();
    }
}
