/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import static io.debezium.connector.vitess.SourceInfo.SHARD_KEY;
import static io.debezium.connector.vitess.SourceInfo.VGTID_KEY;
import static io.debezium.connector.vitess.VgtidTest.TEST_GTID;
import static io.debezium.connector.vitess.VgtidTest.TEST_KEYSPACE;
import static io.debezium.connector.vitess.VgtidTest.TEST_SHARD;
import static io.debezium.connector.vitess.VgtidTest.VGTID_JSON;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.schema.SchemaFactory;

public class UseLocalVgtidTest {

    public static final String LOCAL_VGTID_JSON_WITH_LAST_PK_TEMPLATE = "[" +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\",\"table_p_ks\":[]}" +
            "]";

    public static final String LOCAL_VGTID_JSON_WITH_LAST_PK = String.format(
            LOCAL_VGTID_JSON_WITH_LAST_PK_TEMPLATE,
            TEST_KEYSPACE,
            TEST_SHARD,
            TEST_GTID);

    @Test
    public void shouldNotReplaceATableWithVgtidColumn() {
        Schema recordSchema = SchemaBuilder.struct()
                .field(VGTID_KEY, Schema.STRING_SCHEMA)
                .build();
        Schema sourceSchema = SchemaBuilder.struct()
                .field(VGTID_KEY, Schema.STRING_SCHEMA)
                .field(SHARD_KEY, Schema.STRING_SCHEMA).build();
        Envelope envelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        Schema valueSchema = envelope.schema();
        Struct valueStruct = new Struct(valueSchema)
                .put("before", new Struct(recordSchema).put(VGTID_KEY, "foo"))
                .put("after", new Struct(recordSchema).put(VGTID_KEY, "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema)
                        .put(VGTID_KEY, VGTID_JSON)
                        .put(SHARD_KEY, TEST_SHARD));

        SourceRecord sourceRecord = new SourceRecord(
                null,
                null,
                "topic",
                0,
                null,
                null,
                valueSchema,
                valueStruct,
                null);

        UseLocalVgtid<SourceRecord> useLocalVgtid = new UseLocalVgtid();
        SourceRecord result = useLocalVgtid.apply(sourceRecord);

        Envelope expectedEnvelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        Schema expectedValueSchema = expectedEnvelope.schema();
        Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("before", new Struct(recordSchema).put(VGTID_KEY, "foo"))
                .put("after", new Struct(recordSchema).put(VGTID_KEY, "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema)
                        .put(VGTID_KEY, LOCAL_VGTID_JSON_WITH_LAST_PK)
                        .put(SHARD_KEY, TEST_SHARD));

        assertThat(result.value()).isEqualTo(expectedValueStruct);
    }

    @Test
    public void shouldReplaceWithLocalVgtid() {
        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
        Schema sourceSchema = SchemaBuilder.struct()
                .field(VGTID_KEY, Schema.STRING_SCHEMA)
                .field(SHARD_KEY, Schema.STRING_SCHEMA).build();
        Envelope envelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        Schema valueSchema = envelope.schema();
        Struct valueStruct = new Struct(valueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema)
                        .put(VGTID_KEY, VGTID_JSON)
                        .put(SHARD_KEY, TEST_SHARD));

        SourceRecord sourceRecord = new SourceRecord(
                null,
                null,
                "topic",
                0,
                null,
                null,
                valueSchema,
                valueStruct,
                null);

        UseLocalVgtid<SourceRecord> useLocalVgtid = new UseLocalVgtid();
        SourceRecord result = useLocalVgtid.apply(sourceRecord);

        Envelope expectedEnvelope = SchemaFactory.get().datatypeEnvelopeSchema()
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        Schema expectedValueSchema = expectedEnvelope.schema();
        Struct expectedValueStruct = new Struct(expectedValueSchema)
                .put("before", new Struct(recordSchema).put("id", "foo"))
                .put("after", new Struct(recordSchema).put("id", "foo"))
                .put("op", "c")
                .put("source", new Struct(sourceSchema)
                        .put(VGTID_KEY, LOCAL_VGTID_JSON_WITH_LAST_PK)
                        .put(SHARD_KEY, TEST_SHARD));

        assertThat(result.value()).isEqualTo(expectedValueStruct);
    }
}
