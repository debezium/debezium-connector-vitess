/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.ValueConverter;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.time.MicroTime;
import io.debezium.time.Year;
import io.vitess.proto.Query;

import binlogdata.Binlogdata;

public class VitessValueConverterTest extends VitessTestCleanup {

    private VitessConnectorConfig config;
    private VitessValueConverter converter;
    private VStreamOutputMessageDecoder decoder;

    @Before
    public void before() {
        config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        converter = new VitessValueConverter(
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                config.binaryHandlingMode(),
                config.includeUnknownDatatypes(),
                config.getBigIntUnsgnedHandlingMode());
        schema = new VitessDatabaseSchema(
                config,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(config));
        decoder = new VStreamOutputMessageDecoder(schema);
    }

    public static List<TestHelper.ColumnValue> temporalColumnValues() {
        return Arrays.asList(
                new TestHelper.ColumnValue("time_col", Query.Type.TIME, Types.TIME, "01:02:03".getBytes(), 3600),
                new TestHelper.ColumnValue("year_col", Query.Type.YEAR, Types.INTEGER, "2020".getBytes(), 2020),
                new TestHelper.ColumnValue("datetime_col", Query.Type.DATETIME, Types.TIMESTAMP, "2020-01-01 01:02:03".getBytes(), 3600));
    }

    private static Integer ENUM_VALUE = 1;

    private static Long SET_VALUE = (long) 0b11;

    public static List<TestHelper.ColumnValue> enumColumnValuesString() {
        return Arrays.asList(
                new TestHelper.ColumnValue("enum_col", Query.Type.ENUM, Types.VARCHAR, "a".getBytes(), "a",
                        List.of("a", "b", "c"), "enum('a','b','c')"),
                new TestHelper.ColumnValue("set_col", Query.Type.SET, Types.VARCHAR, "a,b".getBytes(), "a,b",
                        List.of("a", "b", "c"), "set('a','b','c')"));
    }

    public static List<TestHelper.ColumnValue> enumColumnValuesInt() {
        byte[] enumByteArray = ByteBuffer.allocate(Integer.BYTES).putInt(ENUM_VALUE).array();
        byte[] setByteArray = ByteBuffer.allocate(Long.BYTES).putLong(SET_VALUE).array();
        return Arrays.asList(
                new TestHelper.ColumnValue("enum_col", Query.Type.ENUM, Types.VARCHAR, enumByteArray, ENUM_VALUE,
                        List.of("a", "b", "c"), "enum('a','b','c')"),
                new TestHelper.ColumnValue("set_col", Query.Type.SET, Types.VARCHAR, setByteArray, SET_VALUE,
                        List.of("a", "b", "c"), "set('a','b','c')"));
    }

    public static Binlogdata.VEvent temporalFieldEvent() {
        return TestHelper.newFieldEvent(temporalColumnValues());
    }

    public static Binlogdata.VEvent enumFieldEventString() {
        return TestHelper.newFieldEvent(enumColumnValuesString());
    }

    public static Binlogdata.VEvent enumFieldEventInt() {
        return TestHelper.newFieldEvent(enumColumnValuesInt());
    }

    public static List<Query.Field> temporalFields() {
        return TestHelper.newFields(temporalColumnValues());
    }

    @Test
    public void shouldGetInt64SchemaBuilderForTime() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("time_col");
        SchemaBuilder schemaBuilder = converter.schemaBuilder(column);
        assertThat(schemaBuilder.schema()).isEqualTo(MicroTime.builder().build());
        assertThat(schemaBuilder.schema().type()).isEqualTo(SchemaBuilder.INT64_SCHEMA.type());
    }

    @Test
    public void shouldGetYearSchemaBuilderForYear() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("year_col");
        SchemaBuilder schemaBuilder = converter.schemaBuilder(column);
        assertThat(schemaBuilder.schema()).isEqualTo(Year.builder().build());
        assertThat(schemaBuilder.schema().type()).isEqualTo(SchemaBuilder.INT32_SCHEMA.type());
    }

    @Test
    public void shouldConverterWorkForTimeColumn() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("time_col");
        Field field = new Field("foo", 0, Schema.INT64_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        Duration expectedDuration = Duration.ofSeconds(3 + 2 * 60 + 1 * 60 * 60);
        assertThat(valueConverter.convert(expectedDuration)).isInstanceOf(Long.class);
        assertThat(valueConverter.convert(expectedDuration)).isEqualTo(expectedDuration.toNanos() / 1000);
    }

    @Test
    public void shouldConverterWorkForEnumColumnString() throws InterruptedException {
        decoder.processMessage(enumFieldEventString(), null, null, false, true);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("enum_col");
        Field field = new Field("enum", 0, Schema.STRING_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        String expectedValue = "a";
        assertThat(valueConverter.convert(expectedValue)).isInstanceOf(String.class);
        assertThat(valueConverter.convert(expectedValue)).isEqualTo(expectedValue);
    }

    @Test
    public void shouldConverterReturnEmptyStringForInvalid() throws InterruptedException {
        decoder.processMessage(enumFieldEventString(), null, null, false, true);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("enum_col");
        Field field = new Field("enum", 0, Schema.STRING_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        assertThat(valueConverter.convert(0)).isEqualTo("");
        assertThat(valueConverter.convert(20)).isEqualTo("");
    }

    @Test
    public void shouldConverterWorkForEnumColumnIntToString() throws InterruptedException {
        decoder.processMessage(enumFieldEventInt(), null, null, false, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("enum_col");
        Field field = new Field("enum", 0, Schema.STRING_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        Integer expectedValueIndex = ENUM_VALUE;
        String expectedValue = "a";
        assertThat(valueConverter.convert(expectedValueIndex)).isInstanceOf(String.class);
        assertThat(valueConverter.convert(expectedValueIndex)).isEqualTo(expectedValue);
    }

    @Test
    public void shouldConverterWorkForSetColumnString() throws InterruptedException {
        decoder.processMessage(enumFieldEventString(), null, null, false, true);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("set_col");
        Field field = new Field("set", 0, Schema.STRING_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        String expectedValue = "a,b";
        assertThat(valueConverter.convert(expectedValue)).isInstanceOf(String.class);
        assertThat(valueConverter.convert(expectedValue)).isEqualTo(expectedValue);
    }

    @Test
    public void shouldConverterWorkForSetColumnIntToString() throws InterruptedException {
        decoder.processMessage(enumFieldEventInt(), null, null, false, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("set_col");
        Field field = new Field("set", 0, Schema.STRING_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        String expectedValue = "a,b";
        assertThat(valueConverter.convert(SET_VALUE)).isInstanceOf(String.class);
        assertThat(valueConverter.convert(SET_VALUE)).isEqualTo(expectedValue);
    }

    @Test
    public void shouldConverterWorkForDatetimeColumn() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("datetime_col");
        Field field = new Field("foo", 0, Schema.INT64_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        String timestampString = "2020-01-01 01:01:01";
        Timestamp timestamp = Timestamp.valueOf(timestampString);
        Object actual = valueConverter.convert(timestamp);
        assertThat(actual).isInstanceOf(Long.class);
        assertThat(actual).isEqualTo(timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Test
    public void shouldConvertStringToDuration() {
        assertThat(VitessValueConverter.stringToDuration("01:02:03")).isEqualTo(Duration.ofSeconds(1 * 60 * 60 + 2 * 60 + 3));
        assertThat(VitessValueConverter.stringToDuration("01:02:03.1")).isEqualTo(Duration.ofMillis((1 * 60 * 60 + 2 * 60 + 3) * 1000 + 100));
        assertThat(VitessValueConverter.stringToDuration("01:02:03.12")).isEqualTo(Duration.ofMillis((1 * 60 * 60 + 2 * 60 + 3) * 1000 + 120));
        assertThat(VitessValueConverter.stringToDuration("01:02:03.123")).isEqualTo(Duration.ofMillis((1 * 60 * 60 + 2 * 60 + 3) * 1000 + 123));
        assertThat(VitessValueConverter.stringToDuration("01:02:03.1234")).isEqualTo(Duration.ofNanos(
                ((1 * 60 * 60 + 2 * 60 + 3) * 1000L * 1000 + 1234 * 100) * 1000));
        assertThat(VitessValueConverter.stringToDuration("01:02:03.12345")).isEqualTo(Duration.ofNanos(
                ((1 * 60 * 60 + 2 * 60 + 3) * 1000L * 1000 + 12345 * 10) * 1000));
        assertThat(VitessValueConverter.stringToDuration("01:02:03.123456")).isEqualTo(Duration.ofNanos(
                ((1 * 60 * 60 + 2 * 60 + 3) * 1000L * 1000 + 123456) * 1000));
    }

    @Test
    public void shouldConvertStringToLocalDate() {
        assertThat(VitessValueConverter.stringToLocalDate("2020-02-12")).isEqualTo(LocalDate.of(2020, 2, 12));
        assertThat(VitessValueConverter.stringToLocalDate("9999-12-31")).isEqualTo(LocalDate.of(9999, 12, 31));
    }

    @Test
    public void shouldConvertStringToTimestamp() {
        assertThat(VitessValueConverter.stringToTimestamp("2000-01-01 00:00:00")).isEqualTo(
                Timestamp.valueOf(LocalDate.of(2000, 1, 1).atStartOfDay()));
        assertThat(VitessValueConverter.stringToTimestamp("2000-01-01 00:00:00.1")).isEqualTo(
                Timestamp.valueOf(LocalDate.of(2000, 1, 1).atStartOfDay().withNano(1 * 1000 * 1000 * 100)));
        assertThat(VitessValueConverter.stringToTimestamp("2000-01-01 00:00:00.12")).isEqualTo(
                Timestamp.valueOf(LocalDate.of(2000, 1, 1).atStartOfDay().withNano(12 * 1000 * 1000 * 10)));
        assertThat(VitessValueConverter.stringToTimestamp("2000-01-01 00:00:00.123")).isEqualTo(
                Timestamp.valueOf(LocalDate.of(2000, 1, 1).atStartOfDay().withNano(123 * 1000 * 1000)));
        assertThat(VitessValueConverter.stringToTimestamp("2000-01-01 00:00:00.1234")).isEqualTo(
                Timestamp.valueOf(LocalDate.of(2000, 1, 1).atStartOfDay().withNano(1234 * 1000 * 100)));
        assertThat(VitessValueConverter.stringToTimestamp("2000-01-01 00:00:00.12345")).isEqualTo(
                Timestamp.valueOf(LocalDate.of(2000, 1, 1).atStartOfDay().withNano(12345 * 1000 * 10)));
        assertThat(VitessValueConverter.stringToTimestamp("2000-01-01 00:00:00.123456")).isEqualTo(
                Timestamp.valueOf(LocalDate.of(2000, 1, 1).atStartOfDay().withNano(123456 * 1000)));
    }

    @Test
    public void shouldConvertInvalidValueToLocalData() {
        final LogInterceptor logInterceptor = new LogInterceptor(VitessValueConverter.class.getName() + ".invalid_value");
        assertThat(VitessValueConverter.stringToLocalDate("0000-00-00")).isNull();
        assertThat(logInterceptor.containsMessage("Invalid value")).isTrue();
    }
}
