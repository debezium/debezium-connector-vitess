/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.sql.Types;
import java.util.Map;
import java.util.function.Function;

import io.debezium.connector.vitess.VitessType;
import io.debezium.connector.vitess.VitessValueConverter;
import io.debezium.jdbc.TemporalPrecisionMode;

/** Resolve raw column value to Java value */
public class ReplicationMessageColumnValueResolver {

    private static Map<Integer, Function<String, Object>> temporalTypeToConverter = Map.of(
            Types.TIME, VitessValueConverter::stringToDuration,
            Types.DATE, VitessValueConverter::stringToLocalDate,
            Types.TIMESTAMP, VitessValueConverter::stringToTimestamp);

    public static Object resolveValue(VitessType vitessType,
                                      ReplicationMessage.ColumnValue<byte[]> value,
                                      boolean includeUnknownDatatypes,
                                      TemporalPrecisionMode temporalPrecisionMode) {

        if (value.isNull()) {
            return null;
        }

        switch (vitessType.getJdbcId()) {
            case Types.SMALLINT:
                return value.asShort();
            case Types.INTEGER:
                return value.asInteger();
            case Types.BIGINT:
                return value.asLong();
            case Types.BLOB:
            case Types.BINARY:
                return value.asBytes();
            case Types.TIMESTAMP_WITH_TIMEZONE: // This is the case for TIMESTAMP which is simply treated as string
            case Types.VARCHAR:
                return value.asString();
            case Types.FLOAT:
                return value.asFloat();
            case Types.DOUBLE:
                return value.asDouble();
            case Types.TIME:
            case Types.TIMESTAMP: // This is a misnomer and is the case for DATETIME
            case Types.DATE:
                Function<String, Object> converter = temporalTypeToConverter.get(vitessType.getJdbcId());
                return convertOrReturnString(converter, value, temporalPrecisionMode);
            default:
                break;
        }

        return value.asDefault(vitessType, includeUnknownDatatypes);
    }

    private static Object convertOrReturnString(Function<String, Object> converter,
                                                ReplicationMessage.ColumnValue<byte[]> value,
                                                TemporalPrecisionMode temporalPrecisionMode) {
        String stringValue = value.asString();
        if (temporalPrecisionMode.equals(TemporalPrecisionMode.ISOSTRING)) {
            return stringValue;
        }
        else {
            return converter.apply(stringValue);
        }
    }
}
