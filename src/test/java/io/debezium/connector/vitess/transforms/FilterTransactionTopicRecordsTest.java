/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class FilterTransactionTopicRecordsTest {

    @Test
    public void shouldExcludeTransactionTopicMessageWhenEnabled() {
        Schema keySchema = SchemaFactory.get().transactionKeySchema(SchemaNameAdjuster.NO_OP);
        Struct keyStruct = new Struct(keySchema);
        Schema valueSchema = SchemaFactory.get().transactionValueSchema(SchemaNameAdjuster.NO_OP);
        Struct valueStruct = new Struct(valueSchema);
        SourceRecord sourceRecord = new SourceRecord(
                null,
                null,
                "topic",
                0,
                keySchema,
                keyStruct,
                valueSchema,
                valueStruct,
                null);
        FilterTransactionTopicRecords<SourceRecord> exclude = new FilterTransactionTopicRecords<>();
        exclude.configure(Collections.emptyMap());
        SourceRecord resultRecord = exclude.apply(sourceRecord);
        assertThat(resultRecord).isNull();
    }

    @Test
    public void shouldNotExcludeNonTransactionTopicMessages() {
        FilterTransactionTopicRecords<SourceRecord> exclude = new FilterTransactionTopicRecords<>();
        exclude.configure(Collections.emptyMap());
        SourceRecord resultRecord = exclude.apply(TransformsTestHelper.sourceRecord());
        assertThat(resultRecord).isEqualTo(TransformsTestHelper.sourceRecord());
    }

}
