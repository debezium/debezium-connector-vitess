/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.VitessValueConverterTest.temporalFieldEvent;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneOffset;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.relational.Column;
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

    @Before
    public void before() {

        DebeziumOpenLineageEmitter.init(TestHelper.defaultConfig().build(), "test_server");

        VitessConnectorConfig config = new VitessConnectorConfig(
                TestHelper.defaultConfig().with(
                        VitessConnectorConfig.TIME_PRECISION_MODE.name(), TemporalPrecisionMode.ISOSTRING).build());
        converter = new VitessValueConverter(
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                config.binaryHandlingMode(),
                config.includeUnknownDatatypes(),
                config.getBigIntUnsgnedHandlingMode(),
                false);
        schema = new VitessDatabaseSchema(
                config,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(config));
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

}
