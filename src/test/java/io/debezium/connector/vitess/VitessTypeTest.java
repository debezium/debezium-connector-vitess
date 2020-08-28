/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;

import org.junit.Test;

import io.vitess.proto.Query;

public class VitessTypeTest {

    @Test
    public void shouldResolveVitessTypeToJdbcType() {
        assertThat(VitessType.resolve(Query.Type.INT8.name()).getJdbcId()).isEqualTo(Types.SMALLINT);
        assertThat(VitessType.resolve(Query.Type.INT16.name()).getJdbcId()).isEqualTo(Types.SMALLINT);
        assertThat(VitessType.resolve(Query.Type.INT24.name()).getJdbcId()).isEqualTo(Types.INTEGER);
        assertThat(VitessType.resolve(Query.Type.INT32.name()).getJdbcId()).isEqualTo(Types.INTEGER);
        assertThat(VitessType.resolve(Query.Type.INT64.name()).getJdbcId()).isEqualTo(Types.BIGINT);
        assertThat(VitessType.resolve(Query.Type.FLOAT32.name()).getJdbcId()).isEqualTo(Types.FLOAT);
        assertThat(VitessType.resolve(Query.Type.FLOAT64.name()).getJdbcId()).isEqualTo(Types.DOUBLE);
        assertThat(VitessType.resolve(Query.Type.VARBINARY.name()).getJdbcId())
                .isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.BINARY.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.VARCHAR.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.CHAR.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.TEXT.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.JSON.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.DECIMAL.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.TIME.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.DATE.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.DATETIME.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.TIMESTAMP.name()).getJdbcId())
                .isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.ENUM.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve(Query.Type.SET.name()).getJdbcId()).isEqualTo(Types.VARCHAR);
        assertThat(VitessType.resolve("foo").getJdbcId()).isEqualTo(Types.OTHER);
    }
}
