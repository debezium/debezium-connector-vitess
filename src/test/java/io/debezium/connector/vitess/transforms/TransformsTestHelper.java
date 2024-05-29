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
import io.debezium.schema.SchemaFactory;

public class TransformsTestHelper {
    public static Schema RECORD_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .build();
    public static Schema TRANSACTION_SCHEMA = SchemaFactory.get().transactionBlockSchema();
    public static Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA).build();
    public static Envelope ENVELOPE = SchemaFactory.get().datatypeEnvelopeSchema()
            .withRecord(RECORD_SCHEMA)
            .withSource(SOURCE_SCHEMA)
            .withTransaction(TRANSACTION_SCHEMA)
            .build();
    public static Schema KEY_SCHEMA = SchemaBuilder.struct().field("key", SchemaBuilder.STRING_SCHEMA).build();
    public static Struct KEY_STRUCT = new Struct(KEY_SCHEMA).put("key", "k1");
    public static Schema VALUE_SCHEMA = ENVELOPE.schema();
    public static Struct VALUE_STRUCT = new Struct(VALUE_SCHEMA)
            .put("before", new Struct(RECORD_SCHEMA).put("id", "foo"))
            .put("after", new Struct(RECORD_SCHEMA).put("id", "foo"))
            .put("op", "c")
            .put("source", new Struct(SOURCE_SCHEMA).put("db", "bar"))
            .put("transaction", new Struct(TRANSACTION_SCHEMA)
                    .put("id", "foo")
                    .put("data_collection_order", 1L)
                    .put("total_order", 2L));
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
}
