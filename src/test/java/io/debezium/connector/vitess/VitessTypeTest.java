/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(VitessType.resolve(asField(Query.Type.TIME)).getJdbcId()).isEqualTo(Types.TIME);
        assertThat(VitessType.resolve(asField(Query.Type.DATE)).getJdbcId()).isEqualTo(Types.DATE);
        assertThat(VitessType.resolve(asField(Query.Type.DATETIME)).getJdbcId()).isEqualTo(Types.TIMESTAMP);
        assertThat(VitessType.resolve(asField(Query.Type.TIMESTAMP)).getJdbcId())
                .isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
        assertThat(VitessType.resolve(asField(Query.Type.ENUM)).getJdbcId()).isEqualTo(Types.INTEGER);
        assertThat(VitessType.resolve(asField(Query.Type.SET)).getJdbcId()).isEqualTo(Types.BIGINT);
        assertThat(VitessType.resolve(asField(Query.Type.GEOMETRY)).getJdbcId()).isEqualTo(Types.OTHER);
    }

    @Test
    public void shouldResolveVitessTypeWhereColumnTypeDiffers() {
        Query.Field varcharCollateBinary = Query.Field.newBuilder().setType(Query.Type.VARBINARY).setColumnType("varchar(32)").build();
        assertThat(VitessType.resolve(varcharCollateBinary).getJdbcId()).isEqualTo(Types.VARCHAR);
        Query.Field charCollateBinary = Query.Field.newBuilder().setType(Query.Type.BINARY).setColumnType("char(9)").build();
        assertThat(VitessType.resolve(charCollateBinary).getJdbcId()).isEqualTo(Types.VARCHAR);
        Query.Field binary = Query.Field.newBuilder().setType(Query.Type.BINARY).setColumnType("binary(9)").build();
        assertThat(VitessType.resolve(binary).getJdbcId()).isEqualTo(Types.BINARY);
        Query.Field varBinary = Query.Field.newBuilder().setType(Query.Type.VARBINARY).setColumnType("varbinary(9)").build();
        assertThat(VitessType.resolve(varBinary).getJdbcId()).isEqualTo(Types.BINARY);
        Query.Field textCollateBinary = Query.Field.newBuilder().setType(Query.Type.BLOB).setColumnType("text").build();
        assertThat(VitessType.resolve(textCollateBinary).getJdbcId()).isEqualTo(Types.VARCHAR);
        Query.Field mediumtextCollateBinary = Query.Field.newBuilder().setType(Query.Type.BLOB).setColumnType("mediumtext").build();
        assertThat(VitessType.resolve(mediumtextCollateBinary).getJdbcId()).isEqualTo(Types.VARCHAR);
    }

    @Test
    public void shouldResolveEnumToVitessTypeWithIntMappingWhenNotInCopyPhase() {
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
    public void shouldResolveEnumToVitessTypeWithStringValueWhenInCopyPhase() {
        Query.Field enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('eu','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField, true))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.VARCHAR, Arrays.asList("eu", "us", "asia")));

        enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('e,u','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField, true))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.VARCHAR, Arrays.asList("e,u", "us", "asia")));

        enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('e'',u','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField, true))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.VARCHAR, Arrays.asList("e',u", "us", "asia")));

        enumField = Query.Field.newBuilder()
                .setType(Query.Type.ENUM)
                .setColumnType("enum('e'','',''u','us','asia')")
                .build();
        assertThat(VitessType.resolve(enumField, true))
                .isEqualTo(new VitessType(Query.Type.ENUM.name(), Types.VARCHAR, Arrays.asList("e',','u", "us", "asia")));
    }

    @Test
    public void shouldResolveSetToVitessTypeIntWhenNotInCopyPhase() {
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

    @Test
    public void shouldResolveSetToVitessTypeStringWhenInCopyPhase() {
        Query.Field setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('eu','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField, true))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.VARCHAR, Arrays.asList("eu", "us", "asia")));

        setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('e,u','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField, true))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.VARCHAR, Arrays.asList("e,u", "us", "asia")));

        setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('e'',u','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField, true))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.VARCHAR, Arrays.asList("e',u", "us", "asia")));

        setField = Query.Field.newBuilder()
                .setType(Query.Type.SET)
                .setColumnType("set('e'','',''u','us','asia')")
                .build();
        assertThat(VitessType.resolve(setField, true))
                .isEqualTo(new VitessType(Query.Type.SET.name(), Types.VARCHAR, Arrays.asList("e',','u", "us", "asia")));
    }

    private Query.Field asField(Query.Type type) {
        return Query.Field.newBuilder().setType(type).build();
    }

    private Query.Field asFieldWithColumnType(Query.Type type, String columnType) {
        return Query.Field.newBuilder().setType(type).setColumnType(columnType).build();
    }
}
