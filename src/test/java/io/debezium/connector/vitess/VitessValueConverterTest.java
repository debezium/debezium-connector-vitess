/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

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

public class VitessValueConverterTest {

    private VitessDatabaseSchema schema;
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

    public static Binlogdata.VEvent temporalFieldEvent() {
        return TestHelper.newFieldEvent(temporalColumnValues());
    }

    public static List<Query.Field> temporalFields() {
        return TestHelper.newFields(temporalColumnValues());
    }

    @Test
    public void shouldGetInt64SchemaBuilderForTime() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("time_col");
        SchemaBuilder schemaBuilder = converter.schemaBuilder(column);
        assertThat(schemaBuilder.schema()).isEqualTo(MicroTime.builder().build());
        assertThat(schemaBuilder.schema().type()).isEqualTo(SchemaBuilder.INT64_SCHEMA.type());
    }

    @Test
    public void shouldGetYearSchemaBuilderForYear() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("year_col");
        SchemaBuilder schemaBuilder = converter.schemaBuilder(column);
        assertThat(schemaBuilder.schema()).isEqualTo(Year.builder().build());
        assertThat(schemaBuilder.schema().type()).isEqualTo(SchemaBuilder.INT32_SCHEMA.type());
    }

    @Test
    public void shouldConverterWorkForTimeColumn() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("time_col");
        Field field = new Field("foo", 0, Schema.INT64_SCHEMA);
        ValueConverter valueConverter = converter.converter(column, field);
        Duration expectedDuration = Duration.ofSeconds(3 + 2 * 60 + 1 * 60 * 60);
        assertThat(valueConverter.convert(expectedDuration)).isInstanceOf(Long.class);
        assertThat(valueConverter.convert(expectedDuration)).isEqualTo(expectedDuration.toNanos() / 1000);
    }

    @Test
    public void shouldConverterWorkForDatetimeColumn() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false);
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
}
