/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;

import org.junit.Test;

import io.debezium.connector.vitess.AnonymousValue;
import io.debezium.connector.vitess.VitessType;

public class ReplicationMessageColumnValueResolverTest {

    @Test
    public void shouldResolveIntToInt() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.INTEGER),
                new VitessColumnValue("10".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo(10);
    }

    @Test
    public void shouldResolveSmallIntToShort() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.SMALLINT),
                new VitessColumnValue("10".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo((short) 10);
    }

    @Test
    public void shouldResolveBigIntToLong() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.BIGINT),
                new VitessColumnValue("10".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo(10L);
    }

    @Test
    public void shouldResolveVarcharToString() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.VARCHAR),
                new VitessColumnValue("foo".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo("foo");
    }

    @Test
    public void shouldResolveBlobToBytes() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.BLOB),
                new VitessColumnValue("foo".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo("foo".getBytes());
    }

    @Test
    public void shouldResolveBinaryToBytes() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.BINARY),
                new VitessColumnValue("foo".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo("foo".getBytes());
    }

    @Test
    public void shouldResolveFloatToFloat() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.FLOAT),
                new VitessColumnValue("1.1".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo(1.1F);
    }

    @Test
    public void shouldResolveDoubleToDouble() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.DOUBLE),
                new VitessColumnValue("1.1".getBytes()),
                false);
        assertThat(resolvedJavaValue).isEqualTo(1.1D);
    }

    @Test
    public void shouldResolveUnknownToStringWhenNeeded() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.OTHER),
                new VitessColumnValue("foo".getBytes()),
                true);
        assertThat(resolvedJavaValue).isEqualTo("foo");
    }

    @Test
    public void shouldResolveUnknownToNullWhenNeeded() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.OTHER),
                new VitessColumnValue("foo".getBytes()),
                false);
        assertThat(resolvedJavaValue).isNull();
    }
}
