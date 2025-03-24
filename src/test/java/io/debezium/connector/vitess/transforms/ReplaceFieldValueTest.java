/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.schema.SchemaFactory;

/**
 * @author Thomas Thornton
 */
public class ReplaceFieldValueTest {

    private static final String TRANSACTION_BLOCK_SCHEMA_NAME = "event.block";
    public static final int TRANSACTION_BLOCK_SCHEMA_VERSION = 1;

    @Test
    public void shouldReplaceTxIdWhenEnabled() {
        ReplaceFieldValue<SourceRecord> replaceFields = new ReplaceFieldValue<SourceRecord>();
        Map<String, ?> config = Map.of(ReplaceFieldValue.FIELD_NAMES_FIELD.name(), "transaction.id");
        replaceFields.configure(config);
        SourceRecord record = replaceFields.apply(TransformsTestHelper.SOURCE_RECORD_WITH_TRANSACTION);

        Schema expectedTransactionSchema = SchemaFactory.get().transactionBlockSchema();
        Envelope expectedEnvelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(TransformsTestHelper.RECORD_SCHEMA)
                .withSource(TransformsTestHelper.SOURCE_SCHEMA)
                .withTransaction(expectedTransactionSchema)
                .build();
        Schema expectedValueSchema = expectedEnvelope.schema();
        Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("before", new Struct(TransformsTestHelper.RECORD_SCHEMA).put("id", "foo"))
                .put("after", new Struct(TransformsTestHelper.RECORD_SCHEMA).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(TransformsTestHelper.SOURCE_SCHEMA).put("db", "bar"))
                .put("transaction", new Struct(expectedTransactionSchema)
                        .put("id", "")
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));

        Struct actualStruct = (Struct) record.value();
        Schema actualValueSchema = record.valueSchema();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
        assertThat(actualValueSchema).isEqualTo(expectedValueSchema);
    }

    @Test
    public void shouldReplaceTxIdWhenEnabledWithCustomValue() {
        ReplaceFieldValue<SourceRecord> replaceFields = new ReplaceFieldValue<SourceRecord>();
        String customValue = "<redacted>";
        Map<String, ?> config = Map.of(ReplaceFieldValue.FIELD_NAMES_FIELD.name(), "transaction.id",
                ReplaceFieldValue.FIELD_VALUE_FIELD.name(), customValue);
        replaceFields.configure(config);
        SourceRecord record = replaceFields.apply(TransformsTestHelper.SOURCE_RECORD_WITH_TRANSACTION);

        Schema expectedTransactionSchema = SchemaFactory.get().transactionBlockSchema();
        Envelope expectedEnvelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(TransformsTestHelper.RECORD_SCHEMA)
                .withSource(TransformsTestHelper.SOURCE_SCHEMA)
                .withTransaction(expectedTransactionSchema)
                .build();
        Schema expectedValueSchema = expectedEnvelope.schema();
        Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("before", new Struct(TransformsTestHelper.RECORD_SCHEMA).put("id", "foo"))
                .put("after", new Struct(TransformsTestHelper.RECORD_SCHEMA).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(TransformsTestHelper.SOURCE_SCHEMA).put("db", "bar"))
                .put("transaction", new Struct(expectedTransactionSchema)
                        .put("id", customValue)
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));

        Struct actualStruct = (Struct) record.value();
        Schema actualValueSchema = record.valueSchema();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
        assertThat(actualValueSchema).isEqualTo(expectedValueSchema);
    }

    @Test
    public void shouldNotModifyRecordWithMissingFields() {
        ReplaceFieldValue<SourceRecord> replaceFields = new ReplaceFieldValue<SourceRecord>();
        Map<String, ?> config = Map.of(ReplaceFieldValue.FIELD_NAMES_FIELD.name(), "transaction.id");
        replaceFields.configure(config);
        SourceRecord record = replaceFields.apply(TransformsTestHelper.SOURCE_RECORD);

        Schema expectedValueSchema = TransformsTestHelper.ENVELOPE.schema();

        Struct actualStruct = (Struct) record.value();
        Schema actualValueSchema = record.valueSchema();

        assertThat(actualStruct).isEqualToComparingFieldByField(TransformsTestHelper.VALUE_STRUCT);
        assertThat(actualValueSchema).isEqualToComparingFieldByField(expectedValueSchema);
    }

    @Test
    public void shouldNotModifyRecordAlternateStructure() {
        ReplaceFieldValue<SourceRecord> replaceFields = new ReplaceFieldValue<SourceRecord>();
        Map<String, ?> config = Map.of(ReplaceFieldValue.FIELD_NAMES_FIELD.name(), "transaction.id");
        replaceFields.configure(config);
        SourceRecord record = replaceFields.apply(TransformsTestHelper.TRANSACTION_SOURCE_RECORD);

        Struct actualStruct = (Struct) record.value();
        Schema actualValueSchema = record.valueSchema();

        assertThat(actualStruct).isEqualToComparingFieldByField(TransformsTestHelper.TRANSACTION_VALUE_STRUCT);
        assertThat(actualValueSchema).isEqualToComparingFieldByField(TransformsTestHelper.TRANSACTION_VALUE_SCHEMA);
    }

}
