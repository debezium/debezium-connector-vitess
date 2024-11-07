/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.schema.SchemaChangeEvent;

/**
 * @author Thomas Thornton
 */
public class DdlMetadataExtractorTest {

    @Test
    public void shouldGetAlterType() {
        DdlMessage ddlMessage = new DdlMessage(null, null, "ALTER TABLE foo ADD COLUMN bar",
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
    }

    @Test
    public void shouldGetCreateType() {
        DdlMessage ddlMessage = new DdlMessage(null, null, "CREATE    TABLE foo",
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.CREATE);
    }

    @Test
    public void shouldGetTruncateType() {
        DdlMessage ddlMessage = new DdlMessage(null, null, "TRUNCATE    TABLE foo",
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.TRUNCATE);
    }

    @Test
    public void shouldGetTable() {
        DdlMessage ddlMessage = new DdlMessage(null, null, "TRUNCATE    TABLE foo",
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getTable()).isEqualTo("\"0\".\"test_unsharded_keyspace\".\"foo\"");
    }

    @Test
    public void shouldGetDropType() {
        DdlMessage ddlMessage = new DdlMessage(null, null, "DROP TABLE foo",
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.DROP);
    }

    @Test
    public void shouldGetRenameType() {
        DdlMessage ddlMessage = new DdlMessage(null, null, "RENAME TABLE foo TO bar",
                TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_SHARD);
        DdlMetadataExtractor extractor = new DdlMetadataExtractor(ddlMessage);
        assertThat(extractor.getSchemaChangeEventType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
    }

}
