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

    public static Schema recordSchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
    }

    public static Schema transactionSchema() {
        return SchemaFactory.get().transactionBlockSchema();
    }

    public static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .field("db", Schema.STRING_SCHEMA)
                .build();
    }

    public static Envelope envelopeTransaction() {
        return SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema())
                .withSource(sourceSchema())
                .withTransaction(transactionSchema())
                .build();
    }

    public static Envelope envelope() {
        return SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema())
                .withSource(sourceSchema())
                .build();
    }

    public static Schema keySchema() {
        return SchemaBuilder.struct().field("key", SchemaBuilder.STRING_SCHEMA).build();
    }

    public static Struct keyStruct() {
        return new Struct(keySchema()).put("key", "k1");
    }

    public static Schema valueSchemaTransaction() {
        return envelopeTransaction().schema();
    }

    public static Schema valueSchema() {
        return envelope().schema();
    }

    public static Struct valueStructWithTransaction() {
        return new Struct(valueSchemaTransaction())
                .put("before", new Struct(recordSchema()).put("id", "foo"))
                .put("after", new Struct(recordSchema()).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema()).put("db", "bar"))
                .put("transaction", new Struct(transactionSchema())
                        .put("id", "foo")
                        .put("data_collection_order", 1L)
                        .put("total_order", 2L));
    }

    public static Struct valueStruct() {
        return new Struct(valueSchemaTransaction())
                .put("before", new Struct(recordSchema()).put("id", "foo"))
                .put("after", new Struct(recordSchema()).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema()).put("db", "bar"));
    }

    public static SourceRecord sourceRecordWithTransaction() {
        return new SourceRecord(
                null,
                null,
                "topic",
                0,
                keySchema(),
                keyStruct(),
                valueSchemaTransaction(),
                valueStructWithTransaction(),
                null);
    }

    public static SourceRecord sourceRecord() {
        return new SourceRecord(
                null,
                null,
                "topic",
                0,
                keySchema(),
                keyStruct(),
                valueSchema(),
                valueStruct(),
                null);
    }

    public static Schema transactionKeySchema() {
        return SchemaFactory.get().transactionKeySchema(SchemaNameAdjuster.NO_OP);
    }

    public static Struct transactionKeyStruct() {
        return new Struct(transactionKeySchema()).put("id", "gtid");
    }

    public static Schema transactionValueSchema() {
        return SchemaFactory.get().transactionValueSchema(SchemaNameAdjuster.NO_OP);
    }

    public static Struct transactionValueStruct() {
        return new Struct(transactionValueSchema())
                .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY, "status")
                .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, "id")
                .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, 1L)
                .put(TransactionStructMaker.DEBEZIUM_TRANSACTION_TS_MS, 123L);
    }

    public static SourceRecord transactionSourceRecord() {
        return new SourceRecord(
                null,
                null,
                "topic",
                0,
                transactionKeySchema(),
                transactionKeyStruct(),
                transactionValueSchema(),
                transactionValueStruct(),
                null);
    }

}
