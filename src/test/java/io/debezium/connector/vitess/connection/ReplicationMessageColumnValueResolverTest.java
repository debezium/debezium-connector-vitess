/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

import io.debezium.connector.vitess.AnonymousValue;
import io.debezium.connector.vitess.VitessType;
import io.debezium.jdbc.TemporalPrecisionMode;

public class ReplicationMessageColumnValueResolverTest {

    @Test
    public void shouldResolveIntToInt() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.INTEGER),
                new VitessColumnValue("10".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(10);
    }

    @Test
    public void shouldResolveSmallIntToShort() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.SMALLINT),
                new VitessColumnValue("10".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo((short) 10);
    }

    @Test
    public void shouldResolveBigIntToLong() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.BIGINT),
                new VitessColumnValue("10".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(10L);
    }

    @Test
    public void shouldResolveVarcharToString() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.VARCHAR),
                new VitessColumnValue("foo".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo("foo");
    }

    @Test
    public void shouldResolveBlobToBytes() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.BLOB),
                new VitessColumnValue("foo".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo("foo".getBytes());
    }

    @Test
    public void shouldResolveBinaryToBytes() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.BINARY),
                new VitessColumnValue("foo".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo("foo".getBytes());
    }

    @Test
    public void shouldResolveFloatToFloat() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.FLOAT),
                new VitessColumnValue("1.1".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(1.1F);
    }

    @Test
    public void shouldResolveDoubleToDouble() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.DOUBLE),
                new VitessColumnValue("1.1".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(1.1D);
    }

    @Test
    public void shouldResolveUnknownToStringWhenNeeded() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.OTHER),
                new VitessColumnValue("foo".getBytes()),
                true,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo("foo");
    }

    @Test
    public void shouldResolveUnknownToNullWhenNeeded() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.OTHER),
                new VitessColumnValue("foo".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isNull();
    }

    @Test
    public void shouldResolveTimeToDuration() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIME),
                new VitessColumnValue("01:02:03".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(Duration.ofSeconds(3 + 2 * 60 + 1 * 60 * 60));
    }

    @Test
    public void shouldResolveTimeToString() {
        String timeString = "01:02:03";
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIME),
                new VitessColumnValue(timeString.getBytes()),
                false,
                TemporalPrecisionMode.ISOSTRING);
        assertThat(resolvedJavaValue).isEqualTo(timeString);
    }

    @Test
    public void shouldResolveTimeToDurationMilliseconds() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIME),
                new VitessColumnValue("01:02:03.666".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(Duration.ofMillis((3 + 2 * 60 + 1 * 60 * 60) * 1000 + 666));
    }

    @Test
    public void shouldResolveTimeToDurationPrecisionTwo() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIME),
                new VitessColumnValue("01:02:03.66".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(Duration.ofMillis(((3 + 2 * 60 + 1 * 60 * 60) * 100 + 66) * 10));
    }

    @Test
    public void shouldResolveDateToLocalDate() {
        String date = "2020-02-12";
        LocalDate expectedDate = LocalDate.parse(date);
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.DATE),
                new VitessColumnValue(date.getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(expectedDate);
    }

    @Test
    public void shouldResolveDateToString() {
        String date = "2020-02-12";
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.DATE),
                new VitessColumnValue(date.getBytes()),
                false,
                TemporalPrecisionMode.ISOSTRING);
        assertThat(resolvedJavaValue).isEqualTo(date);
    }

    @Test
    public void shouldResolveYearToInt() {
        String year = "2020";
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.INTEGER),
                new VitessColumnValue(year.getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(2020);
    }

    @Test
    public void shouldResolveTimestampToString() {
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIMESTAMP_WITH_TIMEZONE),
                new VitessColumnValue("2020-02-12 01:02:03.1234".getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo("2020-02-12 01:02:03.1234");
    }

    @Test
    public void shouldResolveDatetimeToDurationSeconds() {
        String timestampString = "2020-02-12 01:02:03";
        LocalDateTime timestamp = LocalDateTime.parse(timestampString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        Timestamp expectedTimestamp = Timestamp.valueOf(timestamp);
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIMESTAMP),
                new VitessColumnValue(timestampString.getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(expectedTimestamp);
    }

    @Test
    public void shouldResolveDatetimeToString() {
        String datetimeString = "2020-02-12 01:02:03";
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                // Datetime gets mapped to Types.TIMESTAMP by VitessType
                new VitessType(AnonymousValue.getString(), Types.TIMESTAMP),
                new VitessColumnValue(datetimeString.getBytes()),
                false,
                TemporalPrecisionMode.ISOSTRING);
        assertThat(resolvedJavaValue).isEqualTo(datetimeString);
    }

    @Test
    public void shouldResolveDatetimeToDurationPrecisionFour() {
        String timestampString = "2020-02-12 01:02:03.1234";
        LocalDateTime timestamp = LocalDateTime.parse(timestampString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS"));
        Timestamp expectedTimestamp = Timestamp.valueOf(timestamp);
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIMESTAMP),
                new VitessColumnValue(timestampString.getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(expectedTimestamp);
    }

    @Test
    public void shouldResolveDatetimeToDurationMicros() {
        String timestampString = "2020-02-12 01:02:03.123456";
        LocalDateTime timestamp = LocalDateTime.parse(timestampString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
        Timestamp expectedTimestamp = Timestamp.valueOf(timestamp);
        Object resolvedJavaValue = ReplicationMessageColumnValueResolver.resolveValue(
                new VitessType(AnonymousValue.getString(), Types.TIMESTAMP),
                new VitessColumnValue(timestampString.getBytes()),
                false,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(resolvedJavaValue).isEqualTo(expectedTimestamp);
    }
}
