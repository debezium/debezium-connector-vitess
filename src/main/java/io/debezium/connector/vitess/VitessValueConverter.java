/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.ValueConverter;

/** Used by {@link RelationalChangeRecordEmitter} to convert Java value to Connect value */
public class VitessValueConverter extends JdbcValueConverters {
    private final boolean includeUnknownDatatypes;

    public VitessValueConverter(
                                DecimalMode decimalMode,
                                TemporalPrecisionMode temporalPrecisionMode,
                                ZoneOffset defaultOffset,
                                boolean includeUnknownDatatypes) {
        super(decimalMode, temporalPrecisionMode, defaultOffset, null, null, null);
        this.includeUnknownDatatypes = includeUnknownDatatypes;
    }

    // Get Kafka connect schema from Debebzium column.
    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        final SchemaBuilder jdbcSchemaBuilder = super.schemaBuilder(column);
        if (jdbcSchemaBuilder == null) {
            return includeUnknownDatatypes ? SchemaBuilder.bytes() : null;
        }
        else {
            return jdbcSchemaBuilder;
        }
    }

    // Convert Java value to Kafka connect value.
    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        final ValueConverter jdbcConverter = super.converter(column, fieldDefn);
        if (jdbcConverter == null) {
            return includeUnknownDatatypes
                    ? data -> convertBinary(column, fieldDefn, data, binaryMode)
                    : null;
        }
        else {
            return jdbcConverter;
        }
    }
}
