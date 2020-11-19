/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import io.debezium.annotation.Immutable;
import io.debezium.connector.vitess.VitessType;
import io.debezium.relational.Table;

/**
 * It maps the VStream FIELD to a relational column. A list of ColumnMetaData can be used to create
 * a {@link Table}.
 */
@Immutable
public class ColumnMetaData {
    private final String columnName;
    private final VitessType vitessType;
    private final boolean optional;
    private final KeyMetaData keyMetaData;

    public ColumnMetaData(String columnName, VitessType vitessType, boolean optional, KeyMetaData keyMetaData) {
        this.columnName = columnName;
        this.vitessType = vitessType;
        this.keyMetaData = keyMetaData;
        this.optional = optional;
    }

    public String getColumnName() {
        return columnName;
    }

    public VitessType getVitessType() {
        return vitessType;
    }

    public boolean isOptional() {
        return optional;
    }

    public KeyMetaData getKeyMetaData() {
        return keyMetaData;
    }
}
