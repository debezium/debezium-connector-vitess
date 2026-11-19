/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import io.debezium.relational.Column;

/**
 * Helpers for dealing with partial row images, i.e. rows streamed from a source that runs with
 * {@code binlog_row_image=NOBLOB}.
 */
public final class RowImageUtils {

    private RowImageUtils() {
    }

    /**
     * Whether the column is a BLOB or TEXT column, the only kinds of columns that MySQL omits from
     * a row image when running with {@code binlog_row_image=NOBLOB}.
     *
     * @param column the column to check; may not be null
     * @return true if the column is a BLOB or TEXT type
     */
    public static boolean isBlobOrTextColumn(Column column) {
        final String typeName = column.typeName();
        if (typeName == null) {
            return false;
        }
        final String upperCaseTypeName = typeName.toUpperCase();
        return upperCaseTypeName.contains("BLOB") || upperCaseTypeName.contains("TEXT");
    }
}
