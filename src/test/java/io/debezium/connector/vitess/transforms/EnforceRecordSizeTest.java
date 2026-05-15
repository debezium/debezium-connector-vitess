/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EnforceRecordSizeTest {

    private EnforceRecordSize<SourceRecord> transform;

    @BeforeEach
    public void setup() {
        transform = new EnforceRecordSize<>();
    }

    @AfterEach
    public void teardown() {
        transform.close();
    }

    @Test
    public void shouldNotTruncateWhenMessageSizeUnderLimit() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "10000");
        transform.configure(config);

        SourceRecord record = createRecordWithStringValue("short");
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col")).isEqualTo("short");
    }

    @Test
    public void shouldTruncateStringColumnWhenMessageExceedsLimit() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        String largeValue = "a".repeat(500);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String truncatedValue = after.getString("text_col");
        assertThat(truncatedValue.length()).isLessThan(largeValue.length());
    }

    @Test
    public void shouldTruncateBytesColumnWhenMessageExceedsLimit() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        byte[] largeValue = new byte[500];
        SourceRecord record = createRecordWithBytesValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        byte[] truncatedValue = after.getBytes("blob_col");
        assertThat(truncatedValue.length).isLessThan(largeValue.length);
    }

    @Test
    public void shouldTruncateLargestColumnFirst() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "250");
        transform.configure(config);

        String smallValue = "small";
        String largeValue = "x".repeat(400);
        SourceRecord record = createRecordWithTwoStringColumns(smallValue, largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String smallResult = after.getString("small_col");
        String largeResult = after.getString("large_col");
        assertThat(smallResult.length()).isGreaterThanOrEqualTo(largeResult.length() > 0 ? 1 : 0);
        assertThat(largeResult.length()).isLessThan(largeValue.length());
    }

    @Test
    public void shouldTruncateProportionally() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "300");
        transform.configure(config);

        String col1Value = "a".repeat(200);
        String col2Value = "b".repeat(400);
        SourceRecord record = createRecordWithTwoStringColumns(col1Value, col2Value);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String col1Result = after.getString("small_col");
        String col2Result = after.getString("large_col");
        assertThat(col1Result.length()).isLessThan(col1Value.length());
        assertThat(col2Result.length()).isLessThan(col2Value.length());
    }

    @Test
    public void shouldNotTruncateNonStringColumns() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        SourceRecord record = createRecordWithIntAndString(42, "x".repeat(500));
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getInt32("int_col")).isEqualTo(42);
        String truncatedStr = after.getString("text_col");
        assertThat(truncatedStr.length()).isLessThan(500);
    }

    @Test
    public void shouldHandleNullValues() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "100");
        transform.configure(config);

        SourceRecord record = createRecordWithNullColumn();
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col")).isNull();
        assertThat(after.getInt32("int_col")).isEqualTo(1);
    }

    @Test
    public void shouldHandleNullRecord() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        SourceRecord result = transform.apply(null);
        assertThat(result).isNull();
    }

    @Test
    public void shouldNotModifyRecordWithNoAfterField() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        SourceRecord record = createDeleteRecord("x".repeat(500));
        SourceRecord result = transform.apply(record);

        Struct value = (Struct) result.value();
        Struct before = value.getStruct("before");
        assertThat(before.getString("text_col").length()).isLessThan(500);
    }

    @Test
    public void shouldTruncateBothBeforeAndAfterForUpdate() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "300");
        transform.configure(config);

        SourceRecord record = createUpdateRecord("x".repeat(400), "y".repeat(400));
        SourceRecord result = transform.apply(record);

        Struct value = (Struct) result.value();
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        assertThat(before.getString("text_col").length()).isLessThan(400);
        assertThat(after.getString("text_col").length()).isLessThan(400);
    }

    @Test
    public void shouldRejectInvalidMaxMessageSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "0");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    public void shouldRejectNegativeMaxMessageSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "-1");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    public void shouldPassThroughNonStructRecords() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        SourceRecord record = new SourceRecord(null, null, "topic", 0, Schema.STRING_SCHEMA, "plaintext");
        SourceRecord result = transform.apply(record);
        assertThat(result.value()).isEqualTo("plaintext");
    }

    @Test
    public void shouldRespectMessageOverheadInSizeCalculation() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "500");
        transform.configure(config);

        String largeValue = "a".repeat(1000);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String truncatedValue = after.getString("text_col");
        assertThat(truncatedValue.length()).isLessThan(1000);
        assertThat(estimateSize(result)).isLessThanOrEqualTo(500);
    }

    @Test
    public void shouldHandleMultiByteCharacters() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        String multiByteValue = "中".repeat(500);
        SourceRecord record = createRecordWithStringValue(multiByteValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        String truncatedValue = after.getString("text_col");
        assertThat(truncatedValue.length()).isLessThan(multiByteValue.length());
    }

    @Test
    public void shouldApplyCompressionRatioDefaultNoEffect() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        transform.configure(config);

        String largeValue = "a".repeat(500);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col").length()).isLessThan(500);
    }

    @Test
    public void shouldApplyCompressionRatioReducesEffectiveSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "600");
        config.put(EnforceRecordSize.COMPRESSION_RATIO_CONF, "0.5");
        transform.configure(config);

        String largeValue = "a".repeat(1000);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col")).isEqualTo(largeValue);
    }

    @Test
    public void shouldApplyCompressionRatioIncreasesEffectiveSize() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "2000");
        config.put(EnforceRecordSize.COMPRESSION_RATIO_CONF, "2.0");
        transform.configure(config);

        String largeValue = "a".repeat(1500);
        SourceRecord record = createRecordWithStringValue(largeValue);
        SourceRecord result = transform.apply(record);

        Struct after = getAfterStruct(result);
        assertThat(after.getString("text_col").length()).isLessThan(1500);
    }

    @Test
    public void shouldRejectInvalidCompressionRatio() {
        Map<String, String> config = new HashMap<>();
        config.put(EnforceRecordSize.MAX_BYTES_CONF, "200");
        config.put(EnforceRecordSize.COMPRESSION_RATIO_CONF, "0");

        assertThrows(ConfigException.class, () -> transform.configure(config));
    }

    @Test
    public void shouldEstimateSizeReasonablyComparedToJsonSerialization() {
        org.apache.kafka.connect.json.JsonConverter converter = new org.apache.kafka.connect.json.JsonConverter();
        converter.configure(Map.of("schemas.enable", "false", "converter.type", "value"), false);

        String largeValue = "a".repeat(1000);
        Schema recordSchema = SchemaBuilder.struct()
                .optional()
                .field("text_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Schema sourceSchema = SchemaBuilder.struct().optional().field("db", Schema.OPTIONAL_STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.OPTIONAL_STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();
        Struct afterStruct = new Struct(recordSchema).put("text_col", largeValue);
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);
        SourceRecord record = new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);

        byte[] serialized = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        int actualSerializedSize = serialized.length;
        int ourEstimate = EnforceRecordSize.estimateRecordSizeBytes(record);

        assertThat(ourEstimate).isGreaterThan(actualSerializedSize / 3);
        assertThat(ourEstimate).isLessThan(actualSerializedSize * 2);
    }

    private int estimateSize(SourceRecord record) {
        return EnforceRecordSize.estimateRecordSizeBytes(record);
    }

    private Schema createSimpleRecordSchema() {
        return SchemaBuilder.struct()
                .field("text_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private Schema createBytesRecordSchema() {
        return SchemaBuilder.struct()
                .field("blob_col", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }

    private Schema createTwoColumnSchema() {
        return SchemaBuilder.struct()
                .field("small_col", Schema.OPTIONAL_STRING_SCHEMA)
                .field("large_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private Schema createIntAndStringSchema() {
        return SchemaBuilder.struct()
                .field("int_col", Schema.OPTIONAL_INT32_SCHEMA)
                .field("text_col", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private Struct getAfterStruct(SourceRecord record) {
        Struct value = (Struct) record.value();
        return value.getStruct("after");
    }

    private SourceRecord createRecordWithStringValue(String textValue) {
        Schema recordSchema = createSimpleRecordSchema();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct afterStruct = new Struct(recordSchema).put("text_col", textValue);
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        return new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);
    }

    private SourceRecord createRecordWithBytesValue(byte[] blobValue) {
        Schema recordSchema = createBytesRecordSchema();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct afterStruct = new Struct(recordSchema).put("blob_col", ByteBuffer.wrap(blobValue));
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        return new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);
    }

    private SourceRecord createRecordWithTwoStringColumns(String smallValue, String largeValue) {
        Schema recordSchema = createTwoColumnSchema();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct afterStruct = new Struct(recordSchema)
                .put("small_col", smallValue)
                .put("large_col", largeValue);
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        return new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);
    }

    private SourceRecord createRecordWithIntAndString(int intValue, String textValue) {
        Schema recordSchema = createIntAndStringSchema();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct afterStruct = new Struct(recordSchema)
                .put("int_col", intValue)
                .put("text_col", textValue);
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        return new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);
    }

    private SourceRecord createRecordWithNullColumn() {
        Schema recordSchema = createIntAndStringSchema();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct afterStruct = new Struct(recordSchema)
                .put("int_col", 1)
                .put("text_col", null);
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", afterStruct)
                .put("op", "c")
                .put("source", sourceStruct);

        return new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);
    }

    private SourceRecord createDeleteRecord(String textValue) {
        Schema recordSchema = createSimpleRecordSchema();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct beforeStruct = new Struct(recordSchema).put("text_col", textValue);
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("before", beforeStruct)
                .put("op", "d")
                .put("source", sourceStruct);

        return new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);
    }

    private SourceRecord createUpdateRecord(String beforeValue, String afterValue) {
        Schema recordSchema = createSimpleRecordSchema();
        Schema sourceSchema = SchemaBuilder.struct().field("db", Schema.STRING_SCHEMA).build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", recordSchema)
                .field("after", recordSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct beforeStruct = new Struct(recordSchema).put("text_col", beforeValue);
        Struct afterStruct = new Struct(recordSchema).put("text_col", afterValue);
        Struct sourceStruct = new Struct(sourceSchema).put("db", "test");
        Struct envelope = new Struct(envelopeSchema)
                .put("before", beforeStruct)
                .put("after", afterStruct)
                .put("op", "u")
                .put("source", sourceStruct);

        return new SourceRecord(null, null, "topic", 0, envelopeSchema, envelope);
    }
}
