/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static io.debezium.connector.vitess.connection.ReplicationMessage.Column;

import java.nio.charset.StandardCharsets;

import io.debezium.connector.vitess.VitessType;

/** Logical represenation of both column type and value. */
public class ReplicationMessageColumn implements Column {

    private final String columnName;
    private final VitessType type;
    private final boolean optional;
    private final byte[] rawValue;

    public ReplicationMessageColumn(
                                    String columnName, VitessType type, boolean optional, byte[] rawValue) {
        this.columnName = columnName;
        this.type = type;
        this.optional = optional;
        this.rawValue = rawValue;
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
    public Object getValue(boolean includeUnknownDatatypes) {
        final VitessColumnValue columnValue = new VitessColumnValue(rawValue);

        return ReplicationMessageColumnValueResolver.resolveValue(
                type, columnValue, includeUnknownDatatypes);
    }

    public byte[] getRawValue() {
        return rawValue;
    }

    @Override
    public String toString() {
        return columnName + "=" + new String(rawValue, StandardCharsets.UTF_8);
    }
}
