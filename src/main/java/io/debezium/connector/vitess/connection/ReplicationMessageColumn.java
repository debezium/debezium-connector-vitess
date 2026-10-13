/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static io.debezium.connector.vitess.connection.ReplicationMessage.Column;

import java.nio.charset.StandardCharsets;

import io.debezium.connector.vitess.VitessType;
import io.debezium.connector.vitess.VitessValueConverter;
import io.debezium.jdbc.TemporalPrecisionMode;

/** Logical represenation of both column type and value. */
public class ReplicationMessageColumn implements Column {

    private final String columnName;
    private final VitessType type;
    private final boolean optional;
    private final byte[] rawValue;
    private final boolean unavailable;

    public ReplicationMessageColumn(
                                    String columnName, VitessType type, boolean optional, byte[] rawValue) {
        this(columnName, type, optional, rawValue, false);
    }

    /**
     * @param unavailable whether the value was omitted from the row image by the source, as
     *            happens for unchanged BLOB/TEXT columns with {@code binlog_row_image=NOBLOB};
     *            {@code rawValue} is ignored in that case and {@link #getValue} returns
     *            {@link VitessValueConverter#UNAVAILABLE_VALUE}
     */
    public ReplicationMessageColumn(
                                    String columnName, VitessType type, boolean optional, byte[] rawValue, boolean unavailable) {
        this.columnName = columnName;
        this.type = type;
        this.optional = optional;
        this.rawValue = unavailable ? null : rawValue;
        this.unavailable = unavailable;
    }

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public VitessType getType() {
        return type;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Object getValue(boolean includeUnknownDatatypes, TemporalPrecisionMode temporalPrecisionMode) {
        if (unavailable) {
            return VitessValueConverter.UNAVAILABLE_VALUE;
        }
        final VitessColumnValue columnValue = new VitessColumnValue(rawValue);

        return ReplicationMessageColumnValueResolver.resolveValue(
                type, columnValue, includeUnknownDatatypes, temporalPrecisionMode);
    }

    public byte[] getRawValue() {
        return rawValue;
    }

    public boolean isUnavailable() {
        return unavailable;
    }

    @Override
    public String toString() {
        if (unavailable) {
            return columnName + "=<unavailable>";
        }
        return columnName + "=" + (rawValue == null ? "null" : new String(rawValue, StandardCharsets.UTF_8));
    }
}
