/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;

import io.debezium.connector.vitess.pipeline.txmetadata.Gtid;
import io.debezium.data.Json;
import io.debezium.data.SchemaUtil;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.TableId;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Testing;

public abstract class AbstractVitessConnectorTest extends AbstractAsyncEngineConnectorTest {

    protected static final Pattern INSERT_TABLE_MATCHING_PATTERN = Pattern.compile("insert into (.*)\\(.*\\) VALUES .*", Pattern.CASE_INSENSITIVE);

    protected static final String INSERT_NUMERIC_TYPES_STMT = "INSERT INTO numeric_table ("
            + "tinyint_col,"
            + "tinyint_unsigned_col,"
            + "smallint_col,"
            + "smallint_unsigned_col,"
            + "mediumint_col,"
            + "mediumint_unsigned_col,"
            + "int_col,"
            + "int_unsigned_col,"
            + "bigint_col,"
            + "bigint_unsigned_col,"
            + "bigint_unsigned_overflow_col,"
            + "float_col,"
            + "double_col,"
            + "decimal_col,"
            + "boolean_col)"
            + " VALUES (1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true);";
    protected static final String INSERT_STRING_TYPES_STMT = "INSERT INTO string_table ("
            + "char_col,"
            + "varchar_col,"
            + "varchar_ko_col,"
            + "varchar_ja_col,"
            + "tinytext_col,"
            + "text_col,"
            + "mediumtext_col,"
            + "longtext_col,"
            + "json_col)"
            + " VALUES ('a', 'bc', '상품 명1', 'リンゴ', 'gh', 'ij', 'kl', 'mn', '{\"key1\": \"value1\", \"key2\": {\"key21\": \"value21\", \"key22\": \"value22\"}}');";
    protected static final String INSERT_CHAR_SET_COLLATE_STMT = "INSERT INTO character_set_collate_table (" +
            "varchar_ascii_collate_ascii_bin_col," +
            "varchar_col," +
            "char_ascii_collate_ascii_bin_col," +
            "char_col," +
            "binary_ascii_collate_ascii_bin_col," +
            "varbinary_col," +
            "tinytext_ascii_collate_ascii_bin_col," +
            "tinytext_col," +
            "text_ascii_collate_ascii_bin_col," +
            "text_col," +
            "mediumtext_ascii_collate_ascii_bin_col," +
            "mediumtext_col," +
            "longtext_ascii_collate_ascii_bin_col," +
            "longtext_col," +
            "blob_ascii_collate_ascii_bin_col," +
            "enum_ascii_collate_ascii_bin_col," +
            "enum_col," +
            "set_ascii_collate_ascii_bin_col," +
            "set_col" +
            ") " +
            "VALUES (\"foo\", \"foo\", \"foobarfoo\", \"foobarfoo\", \"foobarfoo\", \"foo\", \"foo\", \"foo\", \"foo\", \"foo\", \"foo\", \"foo\", \"foo\", \"foo\", \"foo\", \"small\", \"small\", \"a\", \"a\");";
    protected static final String INSERT_BYTES_TYPES_STMT = "INSERT INTO string_table ("
            + "binary_col,"
            + "varbinary_col,"
            + "blob_col,"
            + "mediumblob_col,"
            + "longblob_col)"
            + " VALUES ('d', 'ef', 'op', 'qs', 'th');";
    protected static final String INSERT_ENUM_TYPE_STMT = "INSERT INTO enum_table (enum_col)" + " VALUES ('large');";

    protected static final String INSERT_ENUM_AMBIGUOUS_TYPE_STMT = "INSERT INTO enum_ambiguous_table (enum_col)" + " VALUES ('2');";
    protected static final String INSERT_SET_TYPE_STMT = "INSERT INTO set_table (set_col)" + " VALUES ('a,c');";

    protected static final String TIME = "01:02:03";
    protected static final String DATE = "2020-02-11";
    protected static final String DATETIME = "2020-02-12 01:02:03";
    protected static final String TIMESTAMP = "2020-02-13 01:02:03";

    protected static final String ZERO_TIME = "00:00:00";
    protected static final String ZERO_TIME_PRECISION4 = ZERO_TIME + ".0000";
    protected static final String ZERO_DATE = "0000-00-00";
    protected static final String ZERO_DATETIME = "0000-00-00 00:00:00";
    protected static final String ZERO_DATETIME_PRECISION4 = ZERO_DATETIME + ".0000";
    protected static final String ZERO_TIMESTAMP = "0000-00-00 00:00:00";
    protected static final String ZERO_TIMESTAMP_PRECISION6 = ZERO_TIMESTAMP + ".000000";
    protected static final String ZERO_YEAR = "0000";

    protected static final String EPOCH_DATE = "1970-01-01";
    protected static final String EPOCH_DATETIME = "1970-01-01 00:00:00";
    protected static final String EPOCH_DATETIME_PRECISION4 = EPOCH_DATETIME + ".0000";
    protected static final String EPOCH_TIMESTAMP = "1970-01-01 00:00:01"; // MySQL allowed lower bound is one second past the epoch
    protected static final String EPOCH_TIMESTAMP_PRECISION6 = EPOCH_TIMESTAMP + ".000000";
    protected static final String EPOCH_YEAR = "1970";

    protected static final String YEAR = "2020";
    protected static final String INSERT_TIME_TYPES_STMT = "INSERT INTO time_table ("
            + "time_col,"
            + "date_col,"
            + "datetime_col,"
            + "timestamp_col,"
            + "year_col)"
            + String.format(" VALUES ('%s', '%s', '%s', '%s', '%s')", TIME, DATE, DATETIME, TIMESTAMP, YEAR);

    protected static final String INSERT_TIME_TYPES_ZERO_VALUE_STMT = "INSERT INTO time_table_zero_value ("
            + "time_col,"
            + "time_col4,"
            + "date_col,"
            + "datetime_col,"
            + "datetime_col4,"
            + "timestamp_col,"
            + "timestamp_col6,"
            + "year_col)"
            + String.format(" VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                    ZERO_TIME, ZERO_TIME, ZERO_DATE, ZERO_DATETIME, ZERO_DATETIME, ZERO_TIMESTAMP, ZERO_TIMESTAMP, ZERO_YEAR);

    protected static final String INSERT_TIME_TYPES_EPOCH_VALUE_STMT = "INSERT INTO time_table_zero_value ("
            + "time_col,"
            + "time_col4,"
            + "date_col,"
            + "datetime_col,"
            + "datetime_col4,"
            + "timestamp_col,"
            + "timestamp_col6,"
            + "year_col)"
            + String.format(" VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                    ZERO_TIME, ZERO_TIME, EPOCH_DATE, EPOCH_DATETIME, EPOCH_DATETIME, EPOCH_TIMESTAMP, EPOCH_TIMESTAMP, EPOCH_YEAR);

    protected static final String INSERT_TIME_TYPES_ZERO_VALUE_NULLABLE_STMT = INSERT_TIME_TYPES_ZERO_VALUE_STMT.replace(
            "time_table_zero_value", "time_table_zero_value_nullable");

    protected static final String TIME_PRECISION1 = TIME + ".1";
    protected static final String TIME_PRECISION4 = TIME + ".1234";
    protected static final String DATETIME_PRECISION2 = DATETIME + ".12";
    protected static final String DATETIME_PRECISION5 = DATETIME + ".12345";
    protected static final String TIMESTAMP_PRECISION3 = TIMESTAMP + ".123";
    protected static final String TIMESTAMP_PRECISION6 = TIMESTAMP + ".123456";
    protected static final String INSERT_PRECISION_TIME_TYPES_STMT = "INSERT INTO time_table_precision ("
            + "time_col1,"
            + "time_col4,"
            + "datetime_col2,"
            + "datetime_col5,"
            + "timestamp_col3,"
            + "timestamp_col6)"
            + String.format(" VALUES ('%s', '%s', '%s', '%s', '%s', '%s')",
                    TIME_PRECISION1, TIME_PRECISION4, DATETIME_PRECISION2, DATETIME_PRECISION5, TIMESTAMP_PRECISION3, TIMESTAMP_PRECISION6);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    protected List<SchemaAndValueField> schemasAndValuesForNumericTypes() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("tinyint_col", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1),
                        new SchemaAndValueField("tinyint_unsigned_col", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1),
                        new SchemaAndValueField(
                                "smallint_col", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 12),
                        new SchemaAndValueField(
                                "smallint_unsigned_col", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 12),
                        new SchemaAndValueField("mediumint_col", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 123),
                        new SchemaAndValueField("mediumint_unsigned_col", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 123),
                        new SchemaAndValueField("int_col", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 1234),
                        new SchemaAndValueField("int_unsigned_col", SchemaBuilder.OPTIONAL_INT64_SCHEMA, 1234L),
                        new SchemaAndValueField("bigint_col", SchemaBuilder.OPTIONAL_INT64_SCHEMA, 12345L),
                        new SchemaAndValueField("bigint_unsigned_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "12345"),
                        new SchemaAndValueField("bigint_unsigned_overflow_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "18446744073709551615"),
                        new SchemaAndValueField("float_col", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA, 1.5),
                        new SchemaAndValueField("double_col", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA, 2.5),
                        new SchemaAndValueField("decimal_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "12.3400"),
                        new SchemaAndValueField(
                                "boolean_col", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1)));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringTypes() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("char_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "a"),
                        new SchemaAndValueField("varchar_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "bc"),
                        new SchemaAndValueField("varchar_ko_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "상품 명1"),
                        new SchemaAndValueField("varchar_ja_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "リンゴ"),
                        new SchemaAndValueField("tinytext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "gh"),
                        new SchemaAndValueField("text_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "ij"),
                        new SchemaAndValueField("mediumtext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "kl"),
                        new SchemaAndValueField("longtext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "mn"),
                        new SchemaAndValueField("json_col", Json.builder().optional().build(),
                                "{\"key1\":\"value1\",\"key2\":{\"key21\":\"value21\",\"key22\":\"value22\"}}")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForCharSetCollateTypes() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("varchar_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("varchar_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("char_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foobarfoo"),
                        new SchemaAndValueField("char_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foobarfoo"),
                        new SchemaAndValueField("binary_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("foobarfoo".getBytes())),
                        new SchemaAndValueField("varbinary_col", SchemaBuilder.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("foo".getBytes())),
                        new SchemaAndValueField("tinytext_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("tinytext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("text_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("text_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("mediumtext_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("mediumtext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("longtext_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("longtext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "foo"),
                        new SchemaAndValueField("blob_ascii_collate_ascii_bin_col", SchemaBuilder.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("foo".getBytes())),
                        new SchemaAndValueField("enum_ascii_collate_ascii_bin_col", io.debezium.data.Enum.builder("small,medium,large").optional().build(), "small"),
                        new SchemaAndValueField("enum_col", io.debezium.data.Enum.builder("small,medium,large").optional().build(), "small"),
                        new SchemaAndValueField("set_ascii_collate_ascii_bin_col", io.debezium.data.EnumSet.builder("a,b,c,d").optional().build(), "a"),
                        new SchemaAndValueField("set_col", io.debezium.data.EnumSet.builder("a,b,c,d").optional().build(), "a")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringTypesTruncated() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("char_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "a"),
                        new SchemaAndValueField("varchar_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "b"),
                        new SchemaAndValueField("varchar_ko_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "상"),
                        new SchemaAndValueField("varchar_ja_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "リ"),
                        new SchemaAndValueField("tinytext_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "g"),
                        new SchemaAndValueField("text_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "i"),
                        new SchemaAndValueField("mediumtext_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "k"),
                        new SchemaAndValueField("longtext_col", SchemaBuilder.string().optional()
                                .parameter("truncateLength", "1").build(), "m"),
                        new SchemaAndValueField("json_col", Json.builder().optional()
                                .parameter("truncateLength", "1").build(),
                                "{")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringTypesTruncatedBlob() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        ByteBuffer byteBufferTruncated = ByteBuffer.wrap(Arrays.copyOfRange("op".getBytes(), 0, 1));
        ByteBuffer byteBufferTruncatedMedium = ByteBuffer.wrap(Arrays.copyOfRange("qs".getBytes(), 0, 1));
        ByteBuffer byteBufferTruncatedLong = ByteBuffer.wrap(Arrays.copyOfRange("th".getBytes(), 0, 1));
        ByteBuffer byteBufferTruncatedBinary = ByteBuffer.wrap(Arrays.copyOfRange("d".getBytes(), 0, 1));
        ByteBuffer byteBufferTruncatedVarBinary = ByteBuffer.wrap(Arrays.copyOfRange("ef".getBytes(), 0, 1));
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("blob_col", SchemaBuilder.bytes().optional()
                                .parameter("truncateLength", "1").build(), byteBufferTruncated),
                        new SchemaAndValueField("mediumblob_col", SchemaBuilder.bytes().optional()
                                .parameter("truncateLength", "1").build(), byteBufferTruncatedMedium),
                        new SchemaAndValueField("longblob_col", SchemaBuilder.bytes().optional()
                                .parameter("truncateLength", "1").build(), byteBufferTruncatedLong),
                        new SchemaAndValueField("binary_col", SchemaBuilder.bytes().optional()
                                .parameter("truncateLength", "1").build(), byteBufferTruncatedBinary),
                        new SchemaAndValueField("varbinary_col", SchemaBuilder.bytes().optional()
                                .parameter("truncateLength", "1").build(), byteBufferTruncatedVarBinary)));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringTypesExcludedColumn() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("char_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "a"),
                        new SchemaAndValueField("varchar_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "bc"),
                        new SchemaAndValueField("varchar_ko_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "상품 명1"),
                        new SchemaAndValueField("varchar_ja_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "リンゴ"),
                        new SchemaAndValueField("tinytext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "gh"),
                        new SchemaAndValueField("text_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "ij"),
                        new SchemaAndValueField("longtext_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "mn"),
                        new SchemaAndValueField("json_col", Json.builder().optional().build(),
                                "{\"key1\":\"value1\",\"key2\":{\"key21\":\"value21\",\"key22\":\"value22\"}}")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForBytesTypesAsBytes() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("binary_col", SchemaBuilder.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("d\0".getBytes())),
                        new SchemaAndValueField("varbinary_col", SchemaBuilder.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("ef".getBytes())),
                        new SchemaAndValueField("blob_col", SchemaBuilder.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("op".getBytes())),
                        new SchemaAndValueField("mediumblob_col", SchemaBuilder.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("qs".getBytes()))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForBytesTypesAsBase64String() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("binary_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, Base64.getEncoder().encodeToString("d\0".getBytes())),
                        new SchemaAndValueField("varbinary_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, Base64.getEncoder().encodeToString("ef".getBytes())),
                        new SchemaAndValueField("blob_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, Base64.getEncoder().encodeToString("op".getBytes())),
                        new SchemaAndValueField("mediumblob_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, Base64.getEncoder().encodeToString("qs".getBytes()))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForBytesTypesAsHexString() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("binary_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, BaseEncoding.base16().lowerCase().encode("d\0".getBytes())),
                        new SchemaAndValueField("varbinary_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, BaseEncoding.base16().lowerCase().encode("ef".getBytes())),
                        new SchemaAndValueField("blob_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, BaseEncoding.base16().lowerCase().encode("op".getBytes())),
                        new SchemaAndValueField("mediumblob_col", SchemaBuilder.OPTIONAL_STRING_SCHEMA, BaseEncoding.base16().lowerCase().encode("qs".getBytes()))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForEnumType() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("enum_col", io.debezium.data.Enum.builder("small,medium,large").build(), "large")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForEnumTypeAmbiguous() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("enum_col", io.debezium.data.Enum.builder("2,0,1").build(), "2")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForSetType() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("set_col", io.debezium.data.EnumSet.builder("a,b,c,d").build(), "a,c")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypeZeroDate() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col", MicroTime.schema(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("time_col4", MicroTime.schema(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("date_col", io.debezium.time.Date.schema(), getDateIntDays(EPOCH_DATE)),
                        new SchemaAndValueField(
                                "datetime_col", Timestamp.schema(), getMillisForDatetime(EPOCH_DATETIME, 0)),
                        new SchemaAndValueField(
                                "datetime_col4", MicroTimestamp.schema(), getMicrosForDatetime(EPOCH_DATETIME_PRECISION4, 4)),
                        new SchemaAndValueField(
                                "timestamp_col", ZonedTimestamp.schema(), ZERO_TIMESTAMP),
                        new SchemaAndValueField(
                                "timestamp_col6", ZonedTimestamp.schema(), ZERO_TIMESTAMP_PRECISION6),
                        new SchemaAndValueField("year_col", Year.schema(), Integer.valueOf(ZERO_YEAR))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypeZeroDateString() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col", Schema.STRING_SCHEMA, ZERO_TIME),
                        new SchemaAndValueField("time_col4", Schema.STRING_SCHEMA, ZERO_TIME_PRECISION4),
                        new SchemaAndValueField("date_col", Schema.STRING_SCHEMA, ZERO_DATE),
                        new SchemaAndValueField(
                                "datetime_col", Schema.STRING_SCHEMA, ZERO_DATETIME),
                        new SchemaAndValueField(
                                "datetime_col4", Schema.STRING_SCHEMA, ZERO_DATETIME_PRECISION4),
                        new SchemaAndValueField(
                                "timestamp_col", ZonedTimestamp.schema(), ZERO_TIMESTAMP),
                        new SchemaAndValueField(
                                "timestamp_col6", ZonedTimestamp.schema(), ZERO_TIMESTAMP_PRECISION6),
                        new SchemaAndValueField("year_col", Year.schema(), Integer.valueOf(ZERO_YEAR))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypeZeroDateNullable() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col", MicroTime.builder().optional().build(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("time_col4", MicroTime.builder().optional().build(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("date_col", io.debezium.time.Date.builder().optional().schema(), null),
                        new SchemaAndValueField(
                                "datetime_col", Timestamp.builder().optional().schema(), null),
                        new SchemaAndValueField(
                                "datetime_col4", MicroTimestamp.builder().optional().schema(), null),
                        new SchemaAndValueField(
                                "timestamp_col", ZonedTimestamp.builder().optional().schema(), ZERO_TIMESTAMP),
                        new SchemaAndValueField(
                                "timestamp_col6", ZonedTimestamp.builder().optional().schema(), ZERO_TIMESTAMP_PRECISION6),
                        new SchemaAndValueField("year_col", Year.builder().optional().schema(), Integer.valueOf(ZERO_YEAR))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypeZeroDateToNull() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col", MicroTime.schema(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("time_col4", MicroTime.schema(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("date_col", io.debezium.time.Date.builder().optional().schema(), null),
                        new SchemaAndValueField(
                                "datetime_col", Timestamp.builder().optional().schema(), null),
                        new SchemaAndValueField(
                                "datetime_col4", MicroTimestamp.builder().optional().schema(), null),
                        new SchemaAndValueField(
                                "timestamp_col", ZonedTimestamp.schema(), ZERO_TIMESTAMP),
                        new SchemaAndValueField(
                                "timestamp_col6", ZonedTimestamp.schema(), ZERO_TIMESTAMP_PRECISION6),
                        new SchemaAndValueField("year_col", Year.schema(), Integer.valueOf(ZERO_YEAR))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypeTemporalToNullWithEpoch() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col", MicroTime.schema(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("time_col4", MicroTime.schema(), getDurationMicros(ZERO_TIME)),
                        new SchemaAndValueField("date_col", io.debezium.time.Date.builder().optional().schema(), getDateIntDays(EPOCH_DATE)),
                        new SchemaAndValueField(
                                "datetime_col", Timestamp.builder().optional().schema(), getMillisForDatetime(EPOCH_DATETIME, 0)),
                        new SchemaAndValueField(
                                "datetime_col4", MicroTimestamp.builder().optional().schema(), getMicrosForDatetime(EPOCH_DATETIME_PRECISION4, 4)),
                        new SchemaAndValueField(
                                "timestamp_col", ZonedTimestamp.schema(), EPOCH_TIMESTAMP),
                        new SchemaAndValueField(
                                "timestamp_col6", ZonedTimestamp.schema(), EPOCH_TIMESTAMP_PRECISION6),
                        new SchemaAndValueField("year_col", Year.schema(), Integer.valueOf(EPOCH_YEAR))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeType() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col", MicroTime.schema(), getDurationMicros(TIME)),
                        new SchemaAndValueField("date_col", io.debezium.time.Date.schema(), getDateIntDays(DATE)),
                        new SchemaAndValueField(
                                "datetime_col", Timestamp.schema(), getMillisForDatetime(DATETIME, 0)),
                        new SchemaAndValueField(
                                "timestamp_col", ZonedTimestamp.schema(), TIMESTAMP),
                        new SchemaAndValueField("year_col", Year.schema(), Integer.valueOf(YEAR))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypeConnect() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col", Time.SCHEMA, getDateForTime(TIME)),
                        new SchemaAndValueField("date_col", Date.SCHEMA, getDateForDateString(DATE)),
                        new SchemaAndValueField(
                                "datetime_col", org.apache.kafka.connect.data.Timestamp.SCHEMA, getDateForDatetime(DATETIME, 0)),
                        new SchemaAndValueField(
                                "timestamp_col", ZonedTimestamp.schema(), TIMESTAMP),
                        new SchemaAndValueField("year_col", Year.schema(), Integer.valueOf(YEAR))));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypePrecision() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col1", MicroTime.schema(), getDurationMicros(TIME_PRECISION1)),
                        new SchemaAndValueField("time_col4", MicroTime.schema(), getDurationMicros(TIME_PRECISION4)),
                        new SchemaAndValueField(
                                "datetime_col2", Timestamp.schema(), getMillisForDatetime(DATETIME_PRECISION2, 2)),
                        new SchemaAndValueField(
                                "datetime_col5", MicroTimestamp.schema(), getMicrosForDatetime(DATETIME_PRECISION5, 5)),
                        new SchemaAndValueField(
                                "timestamp_col3", ZonedTimestamp.schema(), TIMESTAMP_PRECISION3),
                        new SchemaAndValueField(
                                "timestamp_col6", ZonedTimestamp.schema(), TIMESTAMP_PRECISION6)));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForTimeTypePrecisionConnect() {
        final List<SchemaAndValueField> fields = new ArrayList<>();
        fields.addAll(
                Arrays.asList(
                        new SchemaAndValueField("time_col1", Time.SCHEMA, getDateForTime(TIME_PRECISION1)),
                        new SchemaAndValueField("time_col4", Time.SCHEMA, getDateForTime(TIME_PRECISION4)),
                        new SchemaAndValueField(
                                "datetime_col2", org.apache.kafka.connect.data.Timestamp.SCHEMA, getDateForDatetime(DATETIME_PRECISION2, 2)),
                        new SchemaAndValueField(
                                "datetime_col5", org.apache.kafka.connect.data.Timestamp.SCHEMA, getDateForDatetime(DATETIME_PRECISION5, 5)),
                        new SchemaAndValueField(
                                "timestamp_col3", ZonedTimestamp.schema(), TIMESTAMP_PRECISION3),
                        new SchemaAndValueField(
                                "timestamp_col6", ZonedTimestamp.schema(), TIMESTAMP_PRECISION6)));
        return fields;
    }

    public static java.util.Date getDateForDateString(String date) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format.parse(date);
        }
        catch (ParseException e) {
            return null;
        }
    }

    private long getDurationMicros(String time) {
        return Duration.between(LocalTime.MIN, LocalTime.parse(time)).toNanos() / 1000;
    }

    private java.util.Date getDateForDatetime(String datetime, int precision) {
        return new java.util.Date(getMillisForDatetime(datetime, precision));
    }

    private java.util.Date getDateForTime(String time) {
        return new java.util.Date(getDurationMicros(time) / 1000L);
    }

    private int getDateIntDays(String date) {
        return (int) (getDateForDateString(date).getTime() / (1000 * 60 * 60 * 24));
    }

    private long getMillisForDatetime(String datetime, int precision) {
        return getMicrosForDatetime(datetime, precision) / 1000L;
    }

    private long getMicrosForDatetime(String datetime, int precision) {
        String baseFormat = "yyyy-MM-dd HH:mm:ss";
        for (int i = 0; i < precision; i++) {
            if (i == 0) {
                baseFormat += ".";
            }
            baseFormat += "S";
        }
        Instant instant = LocalDateTime.parse(datetime, DateTimeFormatter.ofPattern(baseFormat)).toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1000L * 1000L + instant.getNano() / 1000L;
    }

    protected static TableId tableIdFromInsertStmt(String statement) {
        return tableIdFromInsertStmt(statement, TestHelper.TEST_UNSHARDED_KEYSPACE);
    }

    protected static TableId tableIdFromInsertStmt(String statement, String keyspace) {
        Matcher matcher = INSERT_TABLE_MATCHING_PATTERN.matcher(statement);
        assertTrue(
                "Extraction of table name from insert statement failed: " + statement, matcher.matches());

        TableId id = TableId.parse(matcher.group(1), false);

        if (id.schema() == null) {
            id = new TableId(id.catalog(), keyspace, id.table());
        }

        return id;
    }

    protected static String topicNameFromInsertStmt(String statement) {
        return topicNameFromInsertStmt(statement, TestHelper.TEST_UNSHARDED_KEYSPACE);
    }

    protected static String topicNameFromInsertStmt(String statement, String keyspace) {
        TableId table = tableIdFromInsertStmt(statement, keyspace);
        String expectedTopicName = table.schema() + "." + table.table();
        return expectedTopicName;
    }

    protected static String incrementGtid(String gtid, int increment) {
        int idx = gtid.lastIndexOf("-") + 1;
        int seq = Integer.valueOf(gtid.substring(idx)) + increment;
        return gtid.substring(0, idx) + seq;
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, String taskId) {
        return getMetricsObjectNameWithTags(connector,
                Collect.linkMapOf("context", getStreamingNamespace(), "server", server, "task", taskId));
    }

    public static void waitForStreamingRunning(String taskId, String connector, String server) {
        try {
            waitForStreamingRunning(taskId != null
                    ? getStreamingMetricsObjectName(connector, server, taskId)
                    : getStreamingMetricsObjectName(connector, server));
        }
        catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitForStreamingRunning(ObjectName objectName) {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        Awaitility.await()
                .alias("Streaming was not started on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> (boolean) mbeanServer.getAttribute(objectName, "Connected"));
    }

    private static ObjectName getMetricsObjectNameWithTags(String connector, Map<String, String> tags) {
        try {
            return new ObjectName(
                    String.format("debezium.%s:%s,type=connector-metrics",
                            connector,
                            tags.entrySet()
                                    .stream()
                                    .map(e -> e.getKey() + "=" + e.getValue())
                                    .collect(Collectors.joining(","))));
        }
        catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    protected TestConsumer testConsumer(int expectedRecordsCount, String... topicPrefixes) throws InterruptedException {
        TestConsumer consumer = new TestConsumer(expectedRecordsCount, topicPrefixes);
        return consumer;
    }

    /** Same as io.debezium.connector.postgresql.AbstractRecordsProducerTest.TestConsumer */
    protected class TestConsumer {
        private final ConcurrentLinkedQueue<SourceRecord> records;
        private int expectedRecordsCount;
        private final List<String> topicPrefixes;
        private boolean ignoreExtraRecords = false;

        protected TestConsumer(int expectedRecordsCount, String... topicPrefixes) {
            this.expectedRecordsCount = expectedRecordsCount;
            this.records = new ConcurrentLinkedQueue<>();
            this.topicPrefixes = Arrays.stream(topicPrefixes)
                    .map(p -> TestHelper.TEST_SERVER + "." + p)
                    .collect(Collectors.toList());
        }

        public void setIgnoreExtraRecords(boolean ignoreExtraRecords) {
            this.ignoreExtraRecords = ignoreExtraRecords;
        }

        public void accept(SourceRecord record) {
            if (ignoreTopic(record.topic())) {
                return;
            }

            if (records.size() >= expectedRecordsCount) {
                addRecord(record);
                if (!ignoreExtraRecords) {
                    fail("received more events than expected");
                }
            }
            else {
                addRecord(record);
            }
        }

        private void addRecord(SourceRecord record) {
            records.add(record);
            if (Testing.Debug.isEnabled()) {
                Testing.debug(
                        "Consumed record "
                                + records.size()
                                + " / "
                                + expectedRecordsCount
                                + " ("
                                + (expectedRecordsCount - records.size())
                                + " more)");
                Testing.debug(record);
            }
            else if (Testing.Print.isEnabled()) {
                Testing.print(
                        "Consumed record "
                                + records.size()
                                + " / "
                                + expectedRecordsCount
                                + " ("
                                + (expectedRecordsCount - records.size())
                                + " more)");
                Testing.print(record);
            }
        }

        private boolean ignoreTopic(String topicName) {
            if (topicPrefixes.isEmpty()) {
                return false;
            }

            for (String prefix : topicPrefixes) {
                if (topicName.startsWith(prefix)) {
                    return false;
                }
            }

            return true;
        }

        protected void expects(int expectedRecordsCount) {
            this.expectedRecordsCount = expectedRecordsCount;
        }

        protected SourceRecord remove() {
            return records.remove();
        }

        protected boolean isEmpty() {
            return records.isEmpty();
        }

        protected void process(Consumer<SourceRecord> consumer) {
            records.forEach(consumer);
        }

        protected void clear() {
            records.clear();
        }

        protected Integer countRecords(long timeout, TimeUnit unit) throws InterruptedException {
            final ElapsedTimeStrategy timer = ElapsedTimeStrategy.constant(Clock.SYSTEM, unit.toMillis(timeout));
            while (!timer.hasElapsed()) {
                final SourceRecord r = consumeRecord();
                if (r != null) {
                    accept(r);
                }
            }
            return records.size();
        }

        protected void await(long timeout, TimeUnit unit) throws InterruptedException {
            await(timeout, timeout / 60, unit);
        }

        protected void await(long timeout, long extraRecordsTimeout, TimeUnit unit) throws InterruptedException {
            final ElapsedTimeStrategy timer = ElapsedTimeStrategy.constant(Clock.SYSTEM, unit.toMillis(timeout));
            while (!timer.hasElapsed()) {
                final SourceRecord r = consumeRecord();
                if (r != null) {
                    accept(r);
                    if (records.size() == expectedRecordsCount && extraRecordsTimeout == 0) {
                        break;
                    }
                    else if (records.size() == expectedRecordsCount) {
                        verifyNoExtraRecords(extraRecordsTimeout, unit);
                        break;
                    }
                }
            }
            if (records.size() != expectedRecordsCount) {
                fail(
                        "Consumer is still expecting "
                                + (expectedRecordsCount - records.size())
                                + " records, as it received only "
                                + records.size());
            }
        }

        private void verifyNoExtraRecords(long extraRecordsTimeout, TimeUnit unit) throws InterruptedException {
            final ElapsedTimeStrategy extraRecordsTimer = ElapsedTimeStrategy.constant(Clock.SYSTEM, unit.toMillis(extraRecordsTimeout));
            while (!extraRecordsTimer.hasElapsed()) {
                final SourceRecord extraRecord = consumeRecord();
                if (extraRecord != null) {
                    accept(extraRecord);
                }
            }
        }
    }

    protected void assertRecordOffset(SourceRecord record) {
        assertRecordOffset(record, RecordOffset.fromSourceInfo(record), false);
    }

    protected void assertRecordOffset(SourceRecord record, boolean hasMultipleShards) {
        assertRecordOffset(record, RecordOffset.fromSourceInfo(record), hasMultipleShards);
    }

    protected void assertRecordOffset(SourceRecord record, RecordOffset expectedRecordOffset) {
        assertRecordOffset(record, expectedRecordOffset, false);
    }

    /**
     * Assert the {@link SourceRecord}'s offset.
     *
     * @param record The {@link SourceRecord} to be checked
     * @param expectedRecordOffset The expected offset
     * @param hasMultipleShards whether the keyspace has multiple shards
     */
    protected void assertRecordOffset(SourceRecord record, RecordOffset expectedRecordOffset, boolean hasMultipleShards) {
        Map<String, ?> offset = record.sourceOffset();
        assertNotNull(offset.get(SourceInfo.VGTID_KEY));
        Object snapshot = offset.get(SourceInfo.SNAPSHOT_KEY);
        assertNull("Snapshot marker not expected, but found", snapshot);

        String offsetVgtidJson = offset.get(SourceInfo.VGTID_KEY).toString();
        String sourceVgtidJson = expectedRecordOffset.vgtid;

        Vgtid offsetVgtid = Vgtid.of(offsetVgtidJson);
        Vgtid sourceVgtid = Vgtid.of(sourceVgtidJson);

        List<Vgtid.ShardGtid> offsetShardGtids = offsetVgtid.getShardGtids();
        assertThat(offsetShardGtids.size()).isEqualTo(sourceVgtid.getShardGtids().size());

        for (Vgtid.ShardGtid offsetShardGtid : offsetShardGtids) {
            String shard = offsetShardGtid.getShard();
            Vgtid.ShardGtid sourceShardGtid = sourceVgtid.getShardGtid(shard);

            String offsetGtidString = offsetShardGtid.getGtid();
            String sourceGtidString = sourceShardGtid.getGtid();

            if (!offsetGtidString.equals("current") && !offsetGtidString.equals("")) {
                Gtid offsetGtid = new Gtid(offsetGtidString);
                Gtid sourceGtid = new Gtid(sourceGtidString);

                Integer offsetSequence = Integer.valueOf(offsetGtid.getSequenceValues().get(0));
                Integer sourceSequence = Integer.valueOf(sourceGtid.getSequenceValues().get(0));

                boolean equals = offsetGtid.equals(sourceGtid);
                boolean isOffsetOneBehindSource = offsetSequence + 1 == sourceSequence;

                assertThat(equals || isOffsetOneBehindSource).isTrue();
            }

        }

        if (hasMultipleShards) {
            assertThat(offsetShardGtids.size() > 1).isTrue();
        }
    }

    protected void assertSourceInfo(SourceRecord record, String name, String keyspace, String table) {
        assertTrue(record.value() instanceof Struct);
        Struct source = ((Struct) record.value()).getStruct("source");
        Assert.assertEquals(name, source.getString(SourceInfo.SERVER_NAME_KEY));
        Assert.assertEquals("", source.getString(SourceInfo.DATABASE_NAME_KEY));
        Assert.assertEquals(keyspace, source.getString(SourceInfo.KEYSPACE_NAME_KEY));
        Assert.assertEquals(table, source.getString(SourceInfo.TABLE_NAME_KEY));
        assertNotNull(source.getString(SourceInfo.VGTID_KEY));
    }

    protected void assertRecordSchemaAndValues(
                                               List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                               SourceRecord record,
                                               String envelopeFieldName) {
        Struct content = ((Struct) record.value()).getStruct(envelopeFieldName);

        assertNotNull("expected there to be content in Envelope under " + envelopeFieldName, content);
        expectedSchemaAndValuesByColumn.forEach(
                schemaAndValueField -> schemaAndValueField.assertFor(content));
    }

    protected static class RecordOffset {
        private final String vgtid;

        public RecordOffset(String vgtid) {
            this.vgtid = vgtid;
        }

        public String getVgtid() {
            return vgtid;
        }

        /**
         * Increment the shard gtid suffix
         */
        public RecordOffset incrementOffset(int increment) {
            Vgtid oldVgtid = Vgtid.of(vgtid);
            Vgtid newVgtid = Vgtid.of(oldVgtid.getShardGtids().stream()
                    .map(shardGtid -> new Vgtid.ShardGtid(shardGtid.getKeyspace(), shardGtid.getShard(), incrementGtid(shardGtid.getGtid(), increment)))
                    .collect(Collectors.toList()));

            return new RecordOffset(newVgtid.toString());
        }

        /**
         * Convert {@link SourceRecord}'s source to offset.
         */
        public static RecordOffset fromSourceInfo(SourceRecord record) {
            if (record.value() instanceof Struct) {
                Struct source = ((Struct) record.value()).getStruct("source");
                return new RecordOffset(source.getString(SourceInfo.VGTID_KEY));
            }
            else {
                throw new IllegalArgumentException("Record value is not a struct");
            }

        }

        /**
         * Assert record's offset.
         */
        protected void assertFor(SourceRecord record) {
            Map<String, ?> offset = record.sourceOffset();
            Assert.assertEquals(vgtid, offset.get(SourceInfo.VGTID_KEY));
        }
    }

    /** Same as io.debezium.connector.postgresql.AbstractRecordsProducerTest.SchemaAndValueField */
    protected static class SchemaAndValueField {
        private final Schema schema;
        private final Object value;

        public String getFieldName() {
            return fieldName;
        }

        private final String fieldName;

        public SchemaAndValueField(String fieldName, Schema schema, Object value) {
            this.schema = schema;
            this.value = value;
            this.fieldName = fieldName;
        }

        protected void assertFor(Struct content) {
            assertSchema(content);
            assertValue(content);
        }

        private void assertValue(Struct content) {
            if (value == null) {
                assertNull(fieldName + " is present in the actual content", content.get(fieldName));
                return;
            }
            Object actualValue = content.get(fieldName);

            // assert the value type; for List all implementation types (e.g. immutable ones) are
            // acceptable
            if (actualValue instanceof List) {
                assertTrue("Incorrect value type for " + fieldName, value instanceof List);
                final List<?> actualValueList = (List<?>) actualValue;
                final List<?> valueList = (List<?>) value;
                assertEquals(
                        "List size don't match for " + fieldName, valueList.size(), actualValueList.size());
                if (!valueList.isEmpty() && valueList.iterator().next() instanceof Struct) {
                    for (int i = 0; i < valueList.size(); i++) {
                        assertStruct((Struct) valueList.get(i), (Struct) actualValueList.get(i));
                    }
                    return;
                }
            }
            else {
                assertEquals(
                        "Incorrect value type for " + fieldName,
                        (value != null) ? value.getClass() : null,
                        (actualValue != null) ? actualValue.getClass() : null);
            }

            if (actualValue instanceof byte[]) {
                assertArrayEquals(
                        "Values don't match for " + fieldName, (byte[]) value, (byte[]) actualValue);
            }
            else if (actualValue instanceof Struct) {
                assertStruct((Struct) value, (Struct) actualValue);
            }
            else {
                Schema schema = content.schema().field(fieldName).schema();
                if (Json.LOGICAL_NAME.equals(schema.name())) {
                    if (value.equals(actualValue)) {
                        // If the string values are identical, don't bother parsing
                        return;
                    }
                    try {
                        JSONAssert.assertEquals("Values don't match for field '" + fieldName + "'", (String) value, (String) actualValue, false);
                    }
                    catch (JSONException e) {
                        throw new ComparisonFailure("Failed to compare JSON field '" + fieldName + "'", (String) value, (String) actualValue);
                    }
                }
                else {
                    assertEquals("Values don't match for field '" + fieldName + "'", value, actualValue);
                }
            }
        }

        private void assertStruct(final Struct expectedStruct, final Struct actualStruct) {
            expectedStruct.schema().fields().stream()
                    .forEach(
                            field -> {
                                final Object expectedValue = expectedStruct.get(field);
                                if (expectedValue == null) {
                                    assertNull(
                                            fieldName + " is present in the actual content",
                                            actualStruct.get(field.name()));
                                    return;
                                }
                                final Object actualValue = actualStruct.get(field.name());
                                assertNotNull("No value found for " + fieldName, actualValue);
                                assertEquals(
                                        "Incorrect value type for " + fieldName,
                                        expectedValue.getClass(),
                                        actualValue.getClass());
                                if (actualValue instanceof byte[]) {
                                    assertArrayEquals(
                                            "Values don't match for " + fieldName,
                                            (byte[]) expectedValue,
                                            (byte[]) actualValue);
                                }
                                else if (actualValue instanceof Struct) {
                                    assertStruct((Struct) expectedValue, (Struct) actualValue);
                                }
                                else {
                                    assertEquals("Values don't match for " + fieldName, expectedValue, actualValue);
                                }
                            });
        }

        private void assertSchema(Struct content) {
            if (schema == null) {
                return;
            }
            Schema schema = content.schema();
            Field field = schema.field(fieldName);
            assertNotNull(fieldName + " not found in schema " + SchemaUtil.asString(schema), field);
            VerifyRecord.assertConnectSchemasAreEqual(field.name(), field.schema(), this.schema);
        }
    }
}
