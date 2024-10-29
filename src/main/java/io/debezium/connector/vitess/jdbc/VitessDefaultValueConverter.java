/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.jdbc;

import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.vitess.VitessValueConverter;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;

/**
 * Create a binlog default value converter to be passed into the {@link io.debezium.relational.TableSchemaBuilder}
 * in {@link io.debezium.connector.vitess.VitessDatabaseSchema}
 * @author Thomas Thornton
 */
public class VitessDefaultValueConverter implements DefaultValueConverter {
    VitessValueConverter converter;

    public VitessDefaultValueConverter(VitessValueConverter converter) {
        this.converter = converter;
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        final SchemaBuilder schemaBuilder = converter.schemaBuilder(column);
        if (schemaBuilder == null) {
            return Optional.of(defaultValueExpression);
        }
        final Field field = new Field(column.name(), -1, schemaBuilder.build());
        final ValueConverter valueConverter = converter.converter(column, field);
        return Optional.ofNullable(valueConverter.convert(defaultValueExpression));
    }
}
