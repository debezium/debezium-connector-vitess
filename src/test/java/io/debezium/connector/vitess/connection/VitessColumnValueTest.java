/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;

import org.junit.Test;

import io.debezium.connector.vitess.VitessType;

public class VitessColumnValueTest {

    @Test
    public void shouldConvertRawValueToBytes() {
        VitessColumnValue value = new VitessColumnValue("1".getBytes());
        assertThat(value.asBytes()).isEqualTo("1".getBytes());
    }

    @Test
    public void shouldConvertRawValueToString() {
        VitessColumnValue value = new VitessColumnValue("1".getBytes());
        assertThat(value.asString()).isEqualTo("1");
    }

    @Test
    public void shouldConvertRawValueToShort() {
        VitessColumnValue value = new VitessColumnValue("1".getBytes());
        assertThat(value.asShort()).isEqualTo((short) 1);
    }

    @Test
    public void shouldConvertRawValueToInteger() {
        VitessColumnValue value = new VitessColumnValue("1".getBytes());
        assertThat(value.asInteger()).isEqualTo(1);
    }

    @Test
    public void shouldConvertRawValueToLong() {
        VitessColumnValue value = new VitessColumnValue("1".getBytes());
        assertThat(value.asLong()).isEqualTo(1L);
    }

    @Test
    public void shouldConvertRawValueToFloat() {
        VitessColumnValue value = new VitessColumnValue("1.2".getBytes());
        assertThat(value.asFloat()).isEqualTo(1.2F);
    }

    @Test
    public void shouldConvertRawValueToDouble() {
        VitessColumnValue value = new VitessColumnValue("1.2".getBytes());
        assertThat(value.asDouble()).isEqualTo(1.2D);
    }

    @Test
    public void shouldConvertRawValueToStringWhenUnknownIsSet() {
        VitessColumnValue value = new VitessColumnValue("1".getBytes());
        assertThat(value.asDefault(new VitessType("foo", Types.OTHER), true)).isEqualTo("1");
    }

    @Test
    public void shouldConvertRawValueToNullWhenUnknownIsUnset() {
        VitessColumnValue value = new VitessColumnValue("1".getBytes());
        assertThat(value.asDefault(new VitessType("foo", Types.OTHER), false)).isNull();
    }
}
