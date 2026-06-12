/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.VitessValueConverterTest.temporalFieldEvent;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.ValueConverter;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * @author Thomas Thornton
 */
public class VitessValueConverterTemporalTypesTest {
    /*
     * Copyright Debezium Authors.
     *
     * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
     */
    private VitessDatabaseSchema schema;
    private VitessValueConverter converter;
    private VStreamOutputMessageDecoder decoder;

    @BeforeEach
    public void before() {
        init(TemporalPrecisionMode.ISOSTRING);
    }

    private void init(TemporalPrecisionMode temporalPrecisionMode) {
        VitessConnectorConfig config = new VitessConnectorConfig(
                TestHelper.defaultConfig().with(
                        VitessConnectorConfig.TIME_PRECISION_MODE.name(), temporalPrecisionMode).build());
        converter = new VitessValueConverter(
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                config.binaryHandlingMode(),
                config.includeUnknownDatatypes(),
                config.getBigIntUnsgnedHandlingMode(),
                false);
        VitessTaskContext taskContext = new VitessTaskContext(TestHelper.defaultConfig().build(), config);
        schema = new VitessDatabaseSchema(
                config,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(config), new CustomConverterRegistry(Collections.emptyList()), taskContext);
        decoder = new VStreamOutputMessageDecoder(schema);
    }

    @Test
    public void shouldGetStringSchemaBuilderForTemporalTypesWithIsoStringTimePrecisionMode() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        for (String columnName : List.of("date_col", "time_col", "datetime_col")) {
            Column column = table.columnWithName(columnName);
            SchemaBuilder schemaBuilder = converter.schemaBuilder(column);
            assertThat(schemaBuilder.schema()).isEqualTo(Schema.STRING_SCHEMA);
        }
    }

    @Test
    public void shouldConvertDatetimeToString() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        String col = "datetime_col";
        Column column = table.columnWithName(col);
        Field field = new Field(col, 2, Schema.STRING_SCHEMA);

        ValueConverter valueConverter = converter.converter(column, field);
        String datetimeString = "0000-00-00 00:00:00";
        Object actual = valueConverter.convert(datetimeString);
        assertThat(actual).isEqualTo(datetimeString);
    }

    @Test
    public void shouldConvertTimeToString() throws InterruptedException {
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        String col = "time_col";
        Column column = table.columnWithName(col);
        Field field = new Field(col, 0, Schema.STRING_SCHEMA);

        ValueConverter valueConverter = converter.converter(column, field);
        String timeString = "00:00:00";
        Object actual = valueConverter.convert(timeString);
        assertThat(actual).isEqualTo(timeString);
    }

    @Test
    public void shouldGetConnectTimestampSchemaBuilderForTimestampWithConnectTimePrecisionMode() throws InterruptedException {
        init(TemporalPrecisionMode.CONNECT);
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        Column column = table.columnWithName("timestamp_col");
        SchemaBuilder schemaBuilder = converter.schemaBuilder(column);
        assertThat(schemaBuilder.schema()).isEqualTo(org.apache.kafka.connect.data.Timestamp.builder().schema());
    }

    @Test
    public void shouldConvertTimestampToDateWithConnectTimePrecisionMode() throws InterruptedException {
        init(TemporalPrecisionMode.CONNECT);
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        String col = "timestamp_col";
        Column column = table.columnWithName(col);
        Field field = new Field(col, 3, org.apache.kafka.connect.data.Timestamp.builder().schema());

        ValueConverter valueConverter = converter.converter(column, field);
        Object actual = valueConverter.convert("2020-01-01 01:02:03");
        assertThat(actual).isEqualTo(java.util.Date.from(
                LocalDateTime.of(2020, 1, 1, 1, 2, 3).toInstant(ZoneOffset.UTC)));
    }

    @Test
    public void shouldConvertZeroTimestampWithConnectTimePrecisionMode() throws InterruptedException {
        init(TemporalPrecisionMode.CONNECT);
        decoder.processMessage(temporalFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        String col = "timestamp_col";
        Column column = table.columnWithName(col);

        // The MySQL zero-date sentinel converts to null for optional columns, like a DATETIME zero-date
        ValueConverter nullableConverter = converter.converter(
                column.edit().optional(true).create(),
                new Field(col, 3, org.apache.kafka.connect.data.Timestamp.builder().optional().schema()));
        assertThat(nullableConverter.convert("0000-00-00 00:00:00")).isNull();

        // For non-optional columns it converts to the epoch fallback
        ValueConverter requiredConverter = converter.converter(
                column.edit().optional(false).create(),
                new Field(col, 3, org.apache.kafka.connect.data.Timestamp.builder().schema()));
        assertThat(requiredConverter.convert("0000-00-00 00:00:00")).isEqualTo(new java.util.Date(0L));
    }

}
