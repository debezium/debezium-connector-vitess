/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.sql.Types;

/** The Vitess table column type */
public class VitessType {

    // name of the column type
    private final String name;
    // enum of column jdbc type
    private final int jdbcId;

    public VitessType(String name, int jdbcId) {
        this.name = name;
        this.jdbcId = jdbcId;
    }

    public String getName() {
        return name;
    }

    public int getJdbcId() {
        return jdbcId;
    }

    @Override
    public String toString() {
        return "VitessType{" + "name='" + name + '\'' + ", jdbcId=" + jdbcId + '}';
    }

    // Resolve JDBC type from vstream FIELD event
    public static VitessType resolve(String columnType) {
        switch (columnType) {
            case "INT8":
            case "INT16":
                return new VitessType(columnType, Types.SMALLINT);
            case "INT24":
            case "INT32":
                return new VitessType(columnType, Types.INTEGER);
            case "INT64":
                return new VitessType(columnType, Types.BIGINT);
            case "VARBINARY":
            case "BINARY":
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            case "JSON":
            case "DECIMAL":
            case "TIME":
            case "DATE":
            case "DATETIME":
            case "TIMESTAMP":
            case "YEAR":
            case "ENUM":
            case "SET":
                return new VitessType(columnType, Types.VARCHAR);
            case "FLOAT32":
                return new VitessType(columnType, Types.FLOAT);
            case "FLOAT64":
                return new VitessType(columnType, Types.DOUBLE);
            default:
                return new VitessType(columnType, Types.OTHER);
        }
    }
}
