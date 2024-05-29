/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.schema.SchemaFactory;

public class RemoveFieldTest {

    private static final String TRANSACTION_BLOCK_SCHEMA_NAME = "event.block";
    public static final int TRANSACTION_BLOCK_SCHEMA_VERSION = 1;

    @Test
    public void shouldExcludeTxIdWhenEnabled() {
        RemoveField<SourceRecord> removeFields = new RemoveField();
        Map<String, ?> config = Map.of(RemoveField.FIELD_NAMES_FIELD.name(), "transaction.id");
        removeFields.configure(config);
        SourceRecord record = removeFields.apply(TransformsTestHelper.SOURCE_RECORD);

        Schema expectedTransactionSchema = SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, SchemaBuilder.int64().build())
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, SchemaBuilder.int64().build())
                .build();
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
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));

        Struct actualStruct = (Struct) record.value();
        Schema actualValueSchema = record.valueSchema();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
        assertThat(actualValueSchema).isEqualTo(expectedValueSchema);
    }

    @Test
    public void shouldExcludeTotalOrderWhenEnabled() {
        RemoveField<SourceRecord> removeFields = new RemoveField();
        Map<String, ?> config = Map.of(RemoveField.FIELD_NAMES_FIELD.name(), "transaction.total_order");
        removeFields.configure(config);
        SourceRecord record = removeFields.apply(TransformsTestHelper.SOURCE_RECORD);

        Schema expectedTransactionSchema = SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, SchemaBuilder.string().build())
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, SchemaBuilder.int64().build())
                .build();
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
                        .put("id", "foo")
                        .put("data_collection_order", 1L));

        Struct actualStruct = (Struct) record.value();
        Schema actualValueSchema = record.valueSchema();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
        assertThat(actualValueSchema).isEqualTo(expectedValueSchema);
    }

    @Test
    public void shouldExcludeDataCollectionOrderWhenEnabled() {

        RemoveField<SourceRecord> removeFields = new RemoveField();
        Map<String, ?> config = Map.of(RemoveField.FIELD_NAMES_FIELD.name(), "transaction.data_collection_order");
        removeFields.configure(config);
        SourceRecord record = removeFields.apply(TransformsTestHelper.SOURCE_RECORD);

        Schema expectedTransactionSchema = SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, SchemaBuilder.string().build())
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, SchemaBuilder.int64().build())
                .build();
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
                        .put("id", "foo")
                        .put("total_order", 2L));

        Struct actualStruct = (Struct) record.value();
        Schema actualValueSchema = record.valueSchema();

        assertThat(actualStruct).isEqualTo(expectedValueStruct);
        assertThat(actualValueSchema).isEqualTo(expectedValueSchema);
    }

    @Test
    public void shouldValidateConfigImproperFormat() {
        RemoveField<SourceRecord> exclude = new RemoveField();
        Map<String, ?> config = Map.of(RemoveField.FIELD_NAMES_FIELD.name(), ".invalid_option");
        assertThatThrownBy(() -> {
            exclude.configure(config);
        }).isInstanceOf(ConfigException.class);
        Map<String, ?> config2 = Map.of(RemoveField.FIELD_NAMES_FIELD.name(), "valid,invalid_option.");
        assertThatThrownBy(() -> {
            exclude.configure(config);
        }).isInstanceOf(ConfigException.class);
    }

    @Test
    public void shouldValidateConfigNoFieldNames() {
        RemoveField<SourceRecord> exclude = new RemoveField();
        Map<String, ?> config = Map.of(RemoveField.FIELD_NAMES_FIELD.name(), "");
        assertThatThrownBy(() -> {
            exclude.configure(config);
        }).isInstanceOf(ConfigException.class);
    }

    @Test
    public void shouldValidateConfigNullFieldNames() {
        RemoveField<SourceRecord> exclude = new RemoveField();
        Map<String, String> config = new HashMap<>();
        config.put(RemoveField.FIELD_NAMES_FIELD.name(), null);
        assertThatThrownBy(() -> {
            exclude.configure(config);
        }).isInstanceOf(ConfigException.class);
    }

    @Test
    public void shouldValidateConfigMultipleFieldNames() {
        RemoveField<SourceRecord> removeField = new RemoveField();
        Map<String, ?> config = Map.of(RemoveField.FIELD_NAMES_FIELD.name(), "foo.bar,foo1");
        removeField.configure(config);
        // assert the list created correctly
        assertThat(removeField.fieldNames).isEqualTo(List.of("foo.bar", "foo1"));
    }

}
