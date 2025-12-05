/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.AnonymousValue;
import io.debezium.connector.vitess.VitessType;
import io.debezium.jdbc.TemporalPrecisionMode;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReplicationMessageColumnTest {

    @Test
    public void shouldGetColumnValue() {
        ReplicationMessageColumn column = new ReplicationMessageColumn(
                AnonymousValue.getString(),
                new VitessType(AnonymousValue.getString(), Types.INTEGER),
                true,
                "10".getBytes());
        Object columnValue = column.getValue(false, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(columnValue).isEqualTo(10);
    }

    @Test
    public void shouldGetExceptionWhenTypeAndValueNotMatch() {
        assertThrows(RuntimeException.class, () -> {
            ReplicationMessageColumn column = new ReplicationMessageColumn(
                    AnonymousValue.getString(),
                    new VitessType(AnonymousValue.getString(), Types.INTEGER),
                    true,
                    "10.1".getBytes());
            column.getValue(false, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        });
    }
}
