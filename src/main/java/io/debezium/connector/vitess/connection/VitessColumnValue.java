/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.VitessType;

/** A convenient wrapper that wraps the raw string value and converts it to Java value. */
public class VitessColumnValue implements ReplicationMessage.ColumnValue<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessColumnValue.class);

    private final String value;

    public VitessColumnValue(String value) {
        this.value = value;
    }

    @Override
    public String getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(value);
    }

    @Override
    public Short asShort() {
        return Short.valueOf(value);
    }

    @Override
    public Long asLong() {
        return Long.valueOf(value);
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(value);
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(value);
    }

    @Override
    public Object asDefault(VitessType vitessType, boolean includeUnknownDatatypes) {
        if (includeUnknownDatatypes) {
            LOGGER.warn("process unknown column type {} as string", vitessType);
            return asString();
        }
        else {
            LOGGER.warn("ignore unknown column type {}", vitessType);
            return null;
        }
    }
}
