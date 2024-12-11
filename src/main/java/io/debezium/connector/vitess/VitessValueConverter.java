/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.data.Json;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Year;
import io.debezium.util.Strings;
import io.vitess.proto.Query;

/** Used by {@link RelationalChangeRecordEmitter} to convert Java value to Connect value */
public class VitessValueConverter extends JdbcValueConverters {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessValueConverter.class);
    private static final Logger INVALID_VALUE_LOGGER = LoggerFactory.getLogger(VitessValueConverter.class.getName() + ".invalid_value");
    private static final BigDecimal BIGINT_MAX_VALUE = new BigDecimal("18446744073709551615");
    private static final BigDecimal BIGINT_CORRECTION = BIGINT_MAX_VALUE.add(BigDecimal.ONE);

    private final boolean includeUnknownDatatypes;
    private final VitessConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode;

    private static final Pattern DATE_FIELD_PATTERN = Pattern.compile("([0-9]*)-([0-9]*)-([0-9]*)");
    private static final Pattern TIME_FIELD_PATTERN = Pattern.compile("(\\-?[0-9]*):([0-9]*)(:([0-9]*))?(\\.([0-9]*))?");

    public VitessValueConverter(
                                DecimalMode decimalMode,
                                TemporalPrecisionMode temporalPrecisionMode,
                                ZoneOffset defaultOffset,
                                BinaryHandlingMode binaryMode,
                                boolean includeUnknownDatatypes,
                                VitessConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode) {
        super(decimalMode, temporalPrecisionMode, defaultOffset, null, null, binaryMode);
        this.includeUnknownDatatypes = includeUnknownDatatypes;
        this.bigIntUnsignedHandlingMode = bigIntUnsignedHandlingMode;
    }

    // Get Kafka connect schema from Debezium column.
    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, Query.Type.JSON.name())) {
            return Json.builder();
        }
        if (matches(typeName, Query.Type.ENUM.name())) {
            return io.debezium.data.Enum.builder(column.enumValues());
        }
        if (matches(typeName, Query.Type.SET.name())) {
            return io.debezium.data.EnumSet.builder(column.enumValues());
        }
        if (matches(typeName, Query.Type.YEAR.name())) {
            return Year.builder();
        }

        if (matches(typeName, Query.Type.UINT64.name())) {
            switch (bigIntUnsignedHandlingMode) {
                case LONG:
                    return SchemaBuilder.int64();
                case STRING:
                    return SchemaBuilder.string();
                case PRECISE:
                    // In order to capture unsigned INT 64-bit data source, org.apache.kafka.connect.data.Decimal:Byte will be required to safely capture all valid values with scale of 0
                    // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
                    return Decimal.builder(0);
                default:
                    throw new IllegalArgumentException("Unknown bigIntUnsignedHandlingMode: " + bigIntUnsignedHandlingMode);
            }
        }

        final SchemaBuilder jdbcSchemaBuilder = super.schemaBuilder(column);
        if (jdbcSchemaBuilder == null) {
            return includeUnknownDatatypes ? SchemaBuilder.bytes() : null;
        }
        else {
            return jdbcSchemaBuilder;
        }
    }

    // Convert Java value to Kafka Connect value.
    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, Query.Type.ENUM.name())) {
            return (data) -> convertEnumToString(column.enumValues(), column, fieldDefn, data);
        }
        if (matches(typeName, Query.Type.SET.name())) {
            return (data) -> convertSetToString(column.enumValues(), column, fieldDefn, data);
        }

        if (matches(typeName, Query.Type.UINT64.name())) {
            switch (bigIntUnsignedHandlingMode) {
                case LONG:
                    return (data) -> convertBigInt(column, fieldDefn, data);
                case STRING:
                    return (data) -> convertString(column, fieldDefn, data);
                case PRECISE:
                    // Convert BIGINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
                    return (data) -> convertUnsignedBigint(column, fieldDefn, data);
                default:
                    throw new IllegalArgumentException("Unknown bigIntUnsignedHandlingMode: " + bigIntUnsignedHandlingMode);
            }
        }

        switch (column.jdbcType()) {
            case Types.TIME:
                if (temporalPrecisionMode == TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS) {
                    return data -> convertDurationToMicroseconds(column, fieldDefn, data);
                }
            case Types.TIMESTAMP:
                return ((ValueConverter) (data -> convertTimestampToLocalDateTime(column, fieldDefn, data))).and(super.converter(column, fieldDefn));
        }

        final ValueConverter jdbcConverter = super.converter(column, fieldDefn);
        if (jdbcConverter == null) {
            return includeUnknownDatatypes
                    ? data -> convertBinary(column, fieldDefn, data, binaryMode)
                    : null;
        }
        else {
            return jdbcConverter;
        }
    }

    protected static Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
        if (data == null && !fieldDefn.schema().isOptional()) {
            return null;
        }
        if (!(data instanceof Timestamp)) {
            return data;
        }

        return ((Timestamp) data).toLocalDateTime();
    }

    protected Object convertDurationToMicroseconds(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                if (data instanceof Duration) {
                    r.deliver(((Duration) data).toNanos() / 1_000);
                }
            }
            catch (IllegalArgumentException e) {
            }
        });
    }

    /**
     * Convert original value insertion of type 'BIGINT' into the correct BIGINT UNSIGNED representation
     * Note: Unsigned BIGINT (64-bit) is represented in 'BigDecimal' data type. Reference: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
     *
     * @param originalNumber {@link BigDecimal} the original insertion value
     * @return {@link BigDecimal} the correct representation of the original insertion value
     */
    protected static BigDecimal convertUnsignedBigint(BigDecimal originalNumber) {
        if (originalNumber.compareTo(BigDecimal.ZERO) == -1) {
            return originalNumber.add(BIGINT_CORRECTION);
        }
        else {
            return originalNumber;
        }
    }

    /**
     * Convert the a value representing a Unsigned BIGINT value to the correct Unsigned INT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the SourceRecord's {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedBigint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            if (data instanceof BigDecimal) {
                r.deliver(convertUnsignedBigint((BigDecimal) data));
            }
            else if (data instanceof Number) {
                r.deliver(convertUnsignedBigint(new BigDecimal(((Number) data).toString())));
            }
            else if (data instanceof String) {
                r.deliver(convertUnsignedBigint(new BigDecimal((String) data)));
            }
            else {
                r.deliver(convertNumeric(column, fieldDefn, data));
            }
        });
    }

    /**
     * Determine if the uppercase form of a column's type exactly matches or begins with the specified prefix.
     * Note that this logic works when the column's {@link Column#typeName() type} contains the type name followed by parentheses.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @param upperCaseMatch the upper case form of the expected type or prefix of the type; may not be null
     * @return {@code true} if the type matches the specified type, or {@code false} otherwise
     */
    protected boolean matches(String upperCaseTypeName, String upperCaseMatch) {
        if (upperCaseTypeName == null) {
            return false;
        }
        return upperCaseMatch.equals(upperCaseTypeName) || upperCaseTypeName.startsWith(upperCaseMatch + "(");
    }

    /**
     * Converts a value object for a MySQL {@code ENUM}, which is represented in the binlog events as an integer value containing
     * the index of the enum option.
     *
     * @param options the characters that appear in the same order as defined in the column; may not be null
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into an {@code ENUM} literal String value
     * @return the converted value, or empty string if the conversion could not be made
     */
    private Object convertEnumToString(List<String> options, Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            // The value has already been converted to a string so deliver as is
            if (data instanceof String) {
                r.deliver(data);
                return;
            }

            // If we don't have options (list of values), we cannot look up an index value
            if (options == null) {
                r.deliver("");
                return;
            }

            // The value is an 1-index int referring to the enum value
            int value = ((Integer) data).intValue();
            int index = value - 1; // 'options' is 0-based
            // an invalid value was specified, which corresponds to the empty string '' and an index of 0
            if (index < options.size() && index >= 0) {
                r.deliver(options.get(index));
            }
            else {
                r.deliver("");
            }
        });
    }

    /**
     * Converts a value object for a MySQL {@code SET}, which is represented in the binlog events contain a long number in which
     * every bit corresponds to a different option.
     *
     * @param options the characters that appear in the same order as defined in the column; may not be null
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into an {@code SET} literal String value
     * @return the converted value, or empty string if the conversion could not be made
     */
    protected Object convertSetToString(List<String> options, Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (data instanceof String) {
                // The value has already been converted to a string so deliver as is
                r.deliver(data);
            }
            else {
                // The binlog will contain a 64-bit bitmask with the indexes of the options in the set value ...
                long indexes = ((Long) data).longValue();
                r.deliver(convertSetValue(column, indexes, options));
            }
        });
    }

    protected String convertSetValue(Column column, long indexes, List<String> options) {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        boolean first = true;
        int optionLen = options.size();
        while (indexes != 0L) {
            if ((indexes & 1) == 1) {
                if (first) {
                    first = false;
                }
                else {
                    sb.append(',');
                }
                if (index < optionLen) {
                    sb.append(options.get(index));
                }
                else {
                    logger.warn("Found unexpected index '{}' on column {}", index, column);
                }
            }
            ++index;
            indexes = indexes >>> 1;
        }
        return sb.toString();
    }

    public static Duration stringToDuration(String timeString) {
        Matcher matcher = TIME_FIELD_PATTERN.matcher(timeString);
        if (!matcher.matches()) {
            throw new DebeziumException("Unexpected format for TIME column: " + timeString);
        }

        boolean isNegative = !timeString.isBlank() && timeString.charAt(0) == '-';

        final long hours = Long.parseLong(matcher.group(1));
        final long minutes = Long.parseLong(matcher.group(2));
        final String secondsGroup = matcher.group(4);
        long seconds = 0;
        long nanoSeconds = 0;

        if (secondsGroup != null) {
            seconds = Long.parseLong(secondsGroup);
            String microSecondsString = matcher.group(6);
            if (microSecondsString != null) {
                nanoSeconds = Long.parseLong(Strings.justifyLeft(microSecondsString, 9, '0'));
            }
        }

        final Duration duration = hours >= 0
                ? Duration
                        .ofHours(hours)
                        .plusMinutes(minutes)
                        .plusSeconds(seconds)
                        .plusNanos(nanoSeconds)
                : Duration
                        .ofHours(hours)
                        .minusMinutes(minutes)
                        .minusSeconds(seconds)
                        .minusNanos(nanoSeconds);
        return isNegative && !duration.isNegative() ? duration.negated() : duration;
    }

    public static LocalDate stringToLocalDate(String dateString) {
        final Matcher matcher = DATE_FIELD_PATTERN.matcher(dateString);
        if (!matcher.matches()) {
            throw new RuntimeException("Unexpected format for DATE column: " + dateString);
        }

        final int year = Integer.parseInt(matcher.group(1));
        final int month = Integer.parseInt(matcher.group(2));
        final int day = Integer.parseInt(matcher.group(3));

        if (year == 0 || month == 0 || day == 0) {
            INVALID_VALUE_LOGGER.warn("Invalid value '{}' stored in column converted to empty value", dateString);
            return null;
        }
        return LocalDate.of(year, month, day);
    }

    public static Timestamp stringToTimestamp(String datetimeString) {
        return Timestamp.valueOf(datetimeString);
    }
}
