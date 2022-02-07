/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;
import java.util.Arrays;

import org.junit.Test;

import io.vitess.proto.Query;

public class VitessTypeTest {

    @Test
    public void shouldResolveVitessTypeToJdbcType() {
        assertThat(VitessType.resolve(asField(Query.Type.INT8)).getJdbcId()).isEqualTo(Types.SMALLINT);
        assertThat(VitessType.resolve(asField(Query.Type.INT16)).getJdbcId()).isEqualTo(Types.SMALLINT);
        assertThat(VitessType.resolve(asField(Query.Type.INT24)).getJdbcId()).isEqualTo(Types.INTEGER);
        assertThat(VitessType.resolve(asField(Query.Type.INT32)).getJdbcId()).isEqualTo(Types.INTEGER);
        assertThat(VitessType.resolve(asField(Query.Type.INT64)).getJdbcId()).isEqualTo(Types.BIGINT);
        assertThat(VitessType.resolve(asField(Query.Type.UINT64)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.FLOAT32)).getJdbcId()).isEqualTo(Types.FLOAT);
        assertThat(VitessType.resolve(asField(Query.Type.FLOAT64)).getJdbcId()).isEqualTo(Types.DOUBLE);
        assertThat(VitessType.resolve(asField(Query.Type.VARBINARY)).getJdbcId())
                .isEqualTo(Types.BINARY);
        assertThat(VitessType.resolve(asField(Query.Type.BINARY)).getJdbcId()).isEqualTo(Types.BINARY);
        assertThat(VitessType.resolve(asField(Query.Type.BLOB)).getJdbcId()).isEqualTo(Types.BLOB);
        assertThat(VitessType.resolve(asField(Query.Type.VARCHAR)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.CHAR)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.TEXT)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.JSON)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.DECIMAL)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.TIME)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.DATE)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.DATETIME)).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.TIMESTAMP)).getJdbcId())
                .isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(asField(Query.Type.ENUM)).getJdbcId()).isEqualTo(Types.INTEGER);
        assertThat(VitessType.resolve(asField(Query.Type.SET)).getJdbcId()).isEqualTo(Types.BIGINT);
        assertThat(VitessType.resolve(asField(Query.Type.GEOMETRY)).getJdbcId()).isEqualTo(Types.OTHER);
    }

    @Test
    public void shouldResolveEnumToVitessType() {
        Query.Field enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('eu','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.INTEGER, Arrays.asList("eu", "us", "asia")));

        enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('e,u','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.INTEGER, Arrays.asList("e,u", "us", "asia")));

        enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('e'',u','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.INTEGER, Arrays.asList("e',u", "us", "asia")));

        enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('e'','',''u','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.INTEGER, Arrays.asList("e',','u", "us", "asia")));
    }

    @Test
    public void shouldResolveSetToVitessType() {
        Query.Field setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('eu','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.BIGINT, Arrays.asList("eu", "us", "asia")));

        setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('e,u','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.BIGINT, Arrays.asList("e,u", "us", "asia")));

        setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('e'',u','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.BIGINT, Arrays.asList("e',u", "us", "asia")));

        setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('e'','',''u','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.BIGINT, Arrays.asList("e',','u", "us", "asia")));
    }

    private Query.Field asField(Query.Type type) {
        return Query.Field.newBuilder().setType(type).build();
    }
}
