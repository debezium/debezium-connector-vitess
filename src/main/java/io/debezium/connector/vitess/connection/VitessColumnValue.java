/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.VitessType;

/** A convenient wrapper that wraps the raw bytes value and converts it to Java value. */
public class VitessColumnValue implements ReplicationMessage.ColumnValue<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessColumnValue.class);

    private final byte[] value;

    public VitessColumnValue(byte[] value) {
        this.value = value;
    }

    @Override
    public byte[] getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public byte[] asBytes() {
        return value;
    }

    @Override
    public String asString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(asString());
    }

    @Override
    public Short asShort() {
        return Short.valueOf(asString());
    }

    @Override
    public Long asLong() {
        return Long.valueOf(asString());
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(asString());
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(asString());
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
