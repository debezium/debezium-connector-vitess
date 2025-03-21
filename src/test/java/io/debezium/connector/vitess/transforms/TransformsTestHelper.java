/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class TransformsTestHelper {

    public static Schema RECORD_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .build();

    public static Schema TRANSACTION_SCHEMA = SchemaFactory.get().transactionBlockSchema();

    public static Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA).build();

    public static Envelope ENVELOPE_TRANSACTION = SchemaFactory.get().datatypeEnvelopeSchema()
            .withRecord(RECORD_SCHEMA)
            .withSource(SOURCE_SCHEMA)
            .withTransaction(TRANSACTION_SCHEMA)
            .build();

    public static Envelope ENVELOPE = SchemaFactory.get().datatypeEnvelopeSchema()
            .withRecord(RECORD_SCHEMA)
            .withSource(SOURCE_SCHEMA)
            .build();

    public static Schema KEY_SCHEMA = SchemaBuilder.struct().field("key", SchemaBuilder.STRING_SCHEMA).build();

    public static Struct KEY_STRUCT = new Struct(KEY_SCHEMA).put("key", "k1");

    public static Schema VALUE_SCHEMA_TRANSACTION = ENVELOPE_TRANSACTION.schema();

    public static Schema VALUE_SCHEMA = ENVELOPE.schema();

    public static Struct VALUE_STRUCT_WITH_TRANSACTION = new Struct(VALUE_SCHEMA_TRANSACTION)
            .put("before", new Struct(RECORD_SCHEMA).put("id", "foo"))
            .put("after", new Struct(RECORD_SCHEMA).put("id", "foo"))
            .put("op", "c")
            .put("source", new Struct(SOURCE_SCHEMA).put("db", "bar"))
            .put("transaction", new Struct(TRANSACTION_SCHEMA)
                    .put("id", "foo")
                    .put("data_collection_order", 1L)
                    .put("total_order", 2L));

    public static Struct VALUE_STRUCT = new Struct(VALUE_SCHEMA_TRANSACTION)
            .put("before", new Struct(RECORD_SCHEMA).put("id", "foo"))
            .put("after", new Struct(RECORD_SCHEMA).put("id", "foo"))
            .put("op", "c")
            .put("source", new Struct(SOURCE_SCHEMA).put("db", "bar"));

    public static SourceRecord SOURCE_RECORD_WITH_TRANSACTION = new SourceRecord(
            null,
            null,
            "topic",
            0,
            KEY_SCHEMA,
            KEY_STRUCT,
            VALUE_SCHEMA_TRANSACTION,
            VALUE_STRUCT_WITH_TRANSACTION,
            null);

    public static SourceRecord SOURCE_RECORD = new SourceRecord(
            null,
            null,
            "topic",
            0,
            KEY_SCHEMA,
            KEY_STRUCT,
            VALUE_SCHEMA,
            VALUE_STRUCT,
            null);

    public static Schema TRANSACTION_KEY_SCHEMA = SchemaFactory.get().transactionKeySchema(SchemaNameAdjuster.NO_OP);

    public static Struct TRANSACTION_KEY_STRUCT = new Struct(TRANSACTION_KEY_SCHEMA).put("id", "gtid");

    public static Schema TRANSACTION_VALUE_SCHEMA = SchemaFactory.get().transactionValueSchema(SchemaNameAdjuster.NO_OP);

    public static Struct TRANSACTION_VALUE_STRUCT = new Struct(TRANSACTION_VALUE_SCHEMA)
            .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY, "status")
            .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, "id")
            .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, 1L)
            .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_TS_MS, 123L);

    public static SourceRecord TRANSACTION_SOURCE_RECORD = new SourceRecord(
            null,
            null,
            "topic",
            0,
            TRANSACTION_KEY_SCHEMA,
            TRANSACTION_KEY_STRUCT,
            TRANSACTION_VALUE_SCHEMA,
            TRANSACTION_VALUE_STRUCT,
            null);
}
