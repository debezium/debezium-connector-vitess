/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.sql.Types;

import io.debezium.connector.vitess.VitessType;
import io.vitess.proto.Query;

/** Resolve raw column value to Java value */
public class ReplicationMessageColumnValueResolver {

    public static Object resolveValue(
                                      VitessType vitessType, ReplicationMessage.ColumnValue value, boolean includeUnknownDatatypes) {
        if (value.isNull()) {
            return null;
        }

        switch (vitessType.getJdbcId()) {
            case Types.SMALLINT:
                return value.asShort();
            case Types.INTEGER:
                return value.asInteger();
            case Types.BIGINT:
                if (vitessType.getName().equals(Query.Type.UINT64.name())) {
                    return Long.parseUnsignedLong(value.asString());
                }
                else {
                    return value.asLong();
                }
            case Types.VARCHAR:
                return value.asString();
            case Types.FLOAT:
                return value.asFloat();
            case Types.DOUBLE:
                return value.asDouble();
            default:
                break;
        }

        return value.asDefault(vitessType, includeUnknownDatatypes);
    }
}
