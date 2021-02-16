/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.data.Json;
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
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return Json.builder();
        }

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

    /**
     * Determine if the uppercase form of a column's type exactly matches or begins with the specified prefix.
     * Note that this logic works when the column's {@link Column#typeName() type} contains the type name followed by parentheses.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @param upperCaseMatch the upper case form of the expected type or prefix of the type; may not be null
     * @return {@code true} if the type matches the specified type, or {@code false} otherwise
     */
    protected boolean matches(String upperCaseTypeName, String upperCaseMatch) {
        if (upperCaseTypeName == null) {
            return false;
        }
        return upperCaseMatch.equals(upperCaseTypeName) || upperCaseTypeName.startsWith(upperCaseMatch + "(");
    }
}
