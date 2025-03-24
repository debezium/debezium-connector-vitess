/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.vitess.proto.Query;

/** The Vitess table column type */
public class VitessType {

    // name of the column type
    private final String name;
    // enum of column jdbc type
    private final int jdbcId;
    // permitted enum values
    private final List<String> enumValues;
    private Optional<Integer> precision;

    public VitessType(String name, int jdbcId) {
        this(name, jdbcId, Collections.emptyList());
    }

    public VitessType(String name, int jdbcId, List<String> enumValues) {
        this(name, jdbcId, enumValues, Optional.empty());
    }

    public VitessType(String name, int jdbcId, Integer precision) {
        this(name, jdbcId, Collections.emptyList(), Optional.of(precision));
    }

    public VitessType(String name, int jdbcId, List<String> enumValues, Optional<Integer> precision) {
        this.name = name;
        this.jdbcId = jdbcId;
        // References the original list, does not create a copy
        this.enumValues = Collections.unmodifiableList(enumValues);
        this.precision = precision;
    }

    public String getName() {
        return name;
    }

    public int getJdbcId() {
        return jdbcId;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public boolean isEnum() {
        return !enumValues.isEmpty();
    }

    @Override
    public String toString() {
        return "VitessType{" +
                "name='" + name + '\'' +
                ", jdbcId=" + jdbcId +
                ", enumValues=" + enumValues +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VitessType that = (VitessType) o;
        return jdbcId == that.jdbcId && name.equals(that.name) && Objects.equals(enumValues, that.enumValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, jdbcId, enumValues);
    }

    public static VitessType resolve(Query.Field field) {
        return resolve(field, false);
    }

    // Resolve JDBC type from vstream FIELD event
    public static VitessType resolve(Query.Field field, boolean isEnumSetStringValue) {
        String type = field.getType().name();
        switch (type) {
            case "INT8":
            case "UINT8":
            case "INT16":
                return new VitessType(type, Types.SMALLINT);
            case "UINT16":
            case "INT24":
            case "UINT24":
            case "INT32":
            case "YEAR":
                return new VitessType(type, Types.INTEGER);
            case "ENUM":
            case "SET":
                return getEnumOrSetVitessType(isEnumSetStringValue, type, field);
            case "UINT32":
            case "INT64":
                return new VitessType(type, Types.BIGINT);
            case "BLOB":
                if (matchAny(field, List.of("TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"))) {
                    return new VitessType(type, Types.VARCHAR);
                }
                return new VitessType(type, Types.BLOB);
            case "VARBINARY":
                if (VitessValueConverter.matches(field.getColumnType().toUpperCase(), "VARCHAR")) {
                    return new VitessType(type, Types.VARCHAR);
                }
            case "BINARY":
                if (VitessValueConverter.matches(field.getColumnType().toUpperCase(), "CHAR")) {
                    return new VitessType(type, Types.VARCHAR);
                }
                else if (VitessValueConverter.matches(field.getColumnType().toUpperCase(), "ENUM")) {
                    return getEnumOrSetVitessType(isEnumSetStringValue, "ENUM", field);
                }
                else if (VitessValueConverter.matches(field.getColumnType().toUpperCase(), "SET")) {
                    return getEnumOrSetVitessType(isEnumSetStringValue, "SET", field);
                }
                return new VitessType(type, Types.BINARY);
            case "UINT64":
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            case "JSON":
            case "DECIMAL":
                return new VitessType(type, Types.VARCHAR);
            case "TIME":
                return new VitessType(type, Types.TIME, field.getDecimals());
            case "DATE":
                return new VitessType(type, Types.DATE);
            case "TIMESTAMP":
                return new VitessType(type, Types.TIMESTAMP_WITH_TIMEZONE, field.getDecimals());
            case "DATETIME":
                return new VitessType(type, Types.TIMESTAMP, field.getDecimals());
            case "FLOAT32":
                return new VitessType(type, Types.FLOAT);
            case "FLOAT64":
                return new VitessType(type, Types.DOUBLE);
            default:
                return new VitessType(type, Types.OTHER);
        }
    }

    private static boolean matchAny(Query.Field field, List<String> types) {
        String upperCaseType = field.getColumnType().toUpperCase();
        return types.stream().filter(type -> VitessValueConverter.matches(upperCaseType, type)).findAny().isPresent();
    }

    private static VitessType getEnumOrSetVitessType(boolean isEnumSetStringValue, String type, Query.Field field) {
        int jdbcType;
        if (type.equals("ENUM") && !isEnumSetStringValue) {
            jdbcType = Types.INTEGER;
        }
        else if (type.equals("SET") && !isEnumSetStringValue) {
            // Set types can have at most 64 items, each item can be present or not, so total is 2^64 so need a BIGINT
            jdbcType = Types.BIGINT;
        }
        else {
            // This is only called when the column type matches either enum or set, so it must be the case that isEnumSetStringValue is true,
            // so we should interpret the value as a string
            jdbcType = Types.VARCHAR;
        }
        return new VitessType(type, jdbcType, resolveEnumAndSetValues(field.getColumnType()));
    }

    /**
     * Resolve the list of permitted Enum or Set values from the Enum or Set Definition
     * @param definition the Enum or Set column definition from the MySQL table. E.g. "enum('m','l','xl')" or "set('a','b','c')"
     * @return The list of permitted Enum values or Set values
     */
    private static List<String> resolveEnumAndSetValues(String definition) {
        List<String> values = new ArrayList<>();
        if (definition == null || definition.length() == 0) {
            return values;
        }

        StringBuilder sb = new StringBuilder();
        boolean startCollecting = false;
        char[] chars = definition.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '\'') {
                if (chars[i + 1] != '\'') {
                    if (startCollecting) {
                        // end of the Enum/Set value, add the Enum/Set value to the result list
                        values.add(sb.toString());
                        sb.setLength(0);
                    }
                    startCollecting = !startCollecting;
                }
                else {
                    sb.append("'");
                    // In MySQL, the single quote in the Enum/Set definition "a'b" is escaped and becomes "a''b".
                    // Skip the second single-quote
                    i++;
                }
            }
            else if (startCollecting) {
                sb.append(chars[i]);
            }
        }
        return values;
    }

    public Optional<Integer> getPrecision() {
        return this.precision;
    }
}
