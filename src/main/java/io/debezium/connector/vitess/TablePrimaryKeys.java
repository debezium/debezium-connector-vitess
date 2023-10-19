/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

import io.vitess.proto.Query;

import binlogdata.Binlogdata;

/**
 * Vitess table copy offsets. During a table copy phase, Vitess VGTID also includes the
 * last primary keys' values for each table. The VGTID received from Vitess, which includes the
 * progress of the table copy, is represented as this class. Used by `Vgtid.java`
 */
public class TablePrimaryKeys {

    public static final String TABLE_NAME_KEY = "table_name";
    public static final String LASTPK_KEY = "lastpk";
    public static final String FIELDS_KEY = "fields";
    public static final String ROWS_KEY = "rows";
    public static final String NAME_KEY = "name";
    public static final String CHARSET_KEY = "charset";
    public static final String TYPE_KEY = "type";
    public static final String FLAGS_KEY = "flags";
    public static final String LENGTHS_KEY = "lengths";
    public static final String VALUES_KEY = "values";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonIgnore
    private final List<Binlogdata.TableLastPK> rawTableLastPrimaryKeys = new ArrayList<>();
    private final List<TableLastPrimaryKey> tableLastPrimaryKeys = new ArrayList<>();

    public TablePrimaryKeys(List<Binlogdata.TableLastPK> rawTableLastPrimaryKey) {
        for (Binlogdata.TableLastPK tableLastPrimaryKey : rawTableLastPrimaryKey) {
            this.rawTableLastPrimaryKeys.add(tableLastPrimaryKey);
            tableLastPrimaryKeys.add(new TableLastPrimaryKey(tableLastPrimaryKey.getTableName(), tableLastPrimaryKey.getLastpk()));
        }
    }

    public static TablePrimaryKeys of(String tablePrimaryKeysJSON) {
        try {
            List<TableLastPrimaryKey> tablePrimaryKeys = MAPPER.readValue(tablePrimaryKeysJSON, new TypeReference<List<TableLastPrimaryKey>>() {
            });
            return createFromTableLastPrimaryKeys(tablePrimaryKeys);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static TablePrimaryKeys createFromRawTableLastPrimaryKey(List<Binlogdata.TableLastPK> rawTableLastPrimaryKeys) {
        return new TablePrimaryKeys(rawTableLastPrimaryKeys);
    }

    public static TablePrimaryKeys createFromTableLastPrimaryKeys(List<TableLastPrimaryKey> tableLastPrimaryKeys) {
        TablePrimaryKeys tablePrimaryKeys = new TablePrimaryKeys();
        if (tableLastPrimaryKeys == null || tableLastPrimaryKeys.size() == 0) {
            return tablePrimaryKeys;
        }
        tablePrimaryKeys.tableLastPrimaryKeys.addAll(tableLastPrimaryKeys);
        for (TableLastPrimaryKey tableLastPrimaryKey : tableLastPrimaryKeys) {
            Binlogdata.TableLastPK rawTableLastPrimaryKey = tableLastPrimaryKey.getRawTableLastPrimaryKey();
            tablePrimaryKeys.rawTableLastPrimaryKeys.add(rawTableLastPrimaryKey);
        }
        return tablePrimaryKeys;
    }

    public List<Binlogdata.TableLastPK> getRawTableLastPrimaryKeys() {
        return rawTableLastPrimaryKeys;
    }

    public List<TableLastPrimaryKey> getTableLastPrimaryKeys() {
        return tableLastPrimaryKeys;
    }

    public TablePrimaryKeys() {
    }

    @Override
    public String toString() {
        try {
            return MAPPER.writeValueAsString(tableLastPrimaryKeys);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawTableLastPrimaryKeys, tableLastPrimaryKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablePrimaryKeys tablePrimaryKeys = (TablePrimaryKeys) o;
        return Objects.equals(rawTableLastPrimaryKeys, tablePrimaryKeys) &&
                Objects.equals(tableLastPrimaryKeys, tablePrimaryKeys.tableLastPrimaryKeys);
    }

    @JsonPropertyOrder({ TABLE_NAME_KEY, LASTPK_KEY })
    public static class TableLastPrimaryKey {

        private final String tableName;
        private final LastPrimaryKey lastPrimaryKey;

        @JsonProperty(TABLE_NAME_KEY)
        public String getTableName() {
            return tableName;
        }

        @JsonProperty(LASTPK_KEY)
        public LastPrimaryKey getLastPrimaryKey() {
            return lastPrimaryKey;
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public TableLastPrimaryKey(@JsonProperty(TABLE_NAME_KEY) String tableName, @JsonProperty(LASTPK_KEY) LastPrimaryKey lastPrimaryKey) {
            this.tableName = tableName;
            this.lastPrimaryKey = lastPrimaryKey;
        }

        public TableLastPrimaryKey(String tableName, Query.QueryResult lastPrimaryKey) {
            this.tableName = tableName;
            this.lastPrimaryKey = new LastPrimaryKey(lastPrimaryKey);
        }

        @JsonIgnore
        public Binlogdata.TableLastPK getRawTableLastPrimaryKey() {
            return Binlogdata.TableLastPK.newBuilder()
                    .setTableName(tableName)
                    .setLastpk(lastPrimaryKey.getRawQueryResult())
                    .build();
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableName, lastPrimaryKey);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableLastPrimaryKey tableLastPrimaryKey = (TableLastPrimaryKey) o;
            return Objects.equals(tableName, tableLastPrimaryKey.tableName) &&
                    Objects.equals(lastPrimaryKey, tableLastPrimaryKey.lastPrimaryKey);
        }
    }

    @JsonPropertyOrder({ FIELDS_KEY, ROWS_KEY })
    public static class LastPrimaryKey {
        private final List<Field> fields = new ArrayList<>();
        private final List<Row> rows = new ArrayList<>();

        public List<Field> getFields() {
            return fields;
        }

        public List<Row> getRows() {
            return rows;
        }

        @JsonCreator
        public LastPrimaryKey(@JsonProperty(FIELDS_KEY) List<Field> fields, @JsonProperty(ROWS_KEY) List<Row> rows) {
            this.fields.addAll(fields);
            this.rows.addAll(rows);
        }

        public LastPrimaryKey(Query.QueryResult queryResult) {
            for (Query.Field field : queryResult.getFieldsList()) {
                fields.add(new Field(field));
            }
            for (Query.Row row : queryResult.getRowsList()) {
                rows.add(new Row(row));
            }
        }

        @JsonIgnore
        public Query.QueryResult getRawQueryResult() {
            return Query.QueryResult.newBuilder()
                    .addAllFields(fields.stream().map(field -> field.getRawField()).collect(Collectors.toList()))
                    .addAllRows(rows.stream().map(row -> row.getRawRow()).collect(Collectors.toList()))
                    .build();
        }

        @Override
        public int hashCode() {
            return Objects.hash(fields, rows);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LastPrimaryKey lastlastPrimaryKey = (LastPrimaryKey) o;
            return Objects.equals(fields, lastlastPrimaryKey.fields) &&
                    Objects.equals(rows, lastlastPrimaryKey.rows);
        }
    }

    @JsonPropertyOrder({ NAME_KEY, TYPE_KEY, CHARSET_KEY, FLAGS_KEY })
    public static class Field {
        private String name;
        private String type;
        private int charset;
        private int flags;

        @JsonCreator
        public Field(@JsonProperty(NAME_KEY) String name, @JsonProperty(TYPE_KEY) String type,
                     @JsonProperty(CHARSET_KEY) int charset, @JsonProperty(FLAGS_KEY) Integer flags) {
            this.name = name;
            this.type = type;
            this.charset = charset;
            this.flags = flags;

        }

        public Field(Query.Field field) {
            name = field.getName();
            type = field.getType().toString();
            charset = field.getCharset();
            flags = field.getFlags();
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public int getCharset() {
            return charset;
        }

        public int getFlags() {
            return flags;
        }

        @JsonIgnore
        public Query.Field getRawField() {
            return Query.Field.newBuilder()
                    .setName(name)
                    .setCharset(charset)
                    .setType(Query.Type.valueOf(type))
                    .setFlags(flags).build();
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, charset, flags);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Field field = (Field) o;
            return Objects.equals(name, field.name) &&
                    Objects.equals(charset, field.charset) &&
                    Objects.equals(type, field.type) &&
                    Objects.equals(flags, field.flags);
        }
    }

    @JsonPropertyOrder({ LENGTHS_KEY, VALUES_KEY })
    public static class Row {
        private List<String> lengths;
        private String values;

        @JsonCreator
        public Row(@JsonProperty(LENGTHS_KEY) List<String> lengths, @JsonProperty(VALUES_KEY) String values) {
            this.lengths = lengths;
            this.values = values;
        }

        public Row(Query.Row row) {
            lengths = row.getLengthsList().stream().map(lengthLong -> lengthLong.toString()).collect(Collectors.toList());
            values = row.getValues().toStringUtf8();
        }

        public List<String> getLengths() {
            return lengths;
        }

        public void setLengths(List<String> lengths) {
            this.lengths = lengths;
        }

        public String getValues() {
            return values;
        }

        public void setValues(String values) {
            this.values = values;
        }

        @JsonIgnore
        public Query.Row getRawRow() {
            try {
                return Query.Row.newBuilder()
                        .addAllLengths(lengths.stream().map(lengthStr -> Long.valueOf(lengthStr)).collect(Collectors.toList()))
                        .setValues(ByteString.copyFrom(values, "UTF-8")).build();
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(lengths, values);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Row row = (Row) o;
            return Objects.equals(lengths, row.lengths) &&
                    Objects.equals(values, row.values);
        }
    }
}
