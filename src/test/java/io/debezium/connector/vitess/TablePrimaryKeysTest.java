package io.debezium.connector.vitess;

import binlogdata.Binlogdata;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import io.vitess.proto.Query;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.assertj.core.api.Assertions.assertThat;


import java.io.UnsupportedEncodingException;
import java.util.List;

public class TablePrimaryKeysTest {

    public static final Binlogdata.TableLastPK getCompPKRawTableLastPK() {
        Query.QueryResult queryResult = null;
        try {
            queryResult = Query.QueryResult.newBuilder()
                    .addFields(Query.Field.newBuilder()
                            .setName("id")
                            .setType(Query.Type.INT64)
                            .setCharset(63)
                            .setFlags(49667))
                    .addFields(Query.Field.newBuilder()
                            .setName("int_col")
                            .setType(Query.Type.INT32)
                            .setCharset(63)
                            .setFlags(53251))
                    .addRows(Query.Row.newBuilder()
                            .addLengths(2)
                            .addLengths(1)
                            .setValues(ByteString.copyFrom("101", "UTF-8"))
                            .build()).build();
            Binlogdata.TableLastPK tableLastPK = Binlogdata.TableLastPK.newBuilder()
                    .setTableName("comp_pk_table")
                    .setLastpk(queryResult).build();
            return tableLastPK;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static final Binlogdata.TableLastPK getNumericRawTableLastPK() {
        Query.QueryResult queryResult = null;
        try {
            queryResult = Query.QueryResult.newBuilder()
                    .addFields(Query.Field.newBuilder()
                            .setName("id")
                            .setType(Query.Type.INT64)
                            .setCharset(63)
                            .setFlags(49667))
                    .addRows(Query.Row.newBuilder()
                            .addLengths(1)
                            .setValues(ByteString.copyFrom("5", "UTF-8"))
                            .build()).build();
            Binlogdata.TableLastPK tableLastPK = Binlogdata.TableLastPK.newBuilder()
                    .setTableName("numeric_table")
                    .setLastpk(queryResult).build();
            return tableLastPK;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static final List<Binlogdata.TableLastPK> getTestRawTableLastPKList() {
        return List.of(getCompPKRawTableLastPK());
    }

    public static List<TablePrimaryKeys.TableLastPrimaryKey> getTestTablePKs(List<Binlogdata.TableLastPK> tableLastPKs) {
        TablePrimaryKeys tablePrimaryKeys = new TablePrimaryKeys(tableLastPKs);
        return tablePrimaryKeys.getTableLastPrimaryKeys();
    }

    public static List<TablePrimaryKeys.TableLastPrimaryKey> getTestTablePKs() {
        TablePrimaryKeys tablePrimaryKeys = new TablePrimaryKeys(getTestRawTableLastPKList());
        return tablePrimaryKeys.getTableLastPrimaryKeys();
    }

    public static final String TEST_COMP_PK_LAST_PKS_JSON = "{\n" +
            "      \"table_name\": \"comp_pk_table\",\n" +
            "      \"lastpk\": {\n" +
            "        \"fields\": [" +
            "            {\n" +
            "            \"name\": \"id\",\n" +
            "            \"type\": \"INT64\",\n" +
            "            \"charset\": 63,\n" +
            "            \"flags\": 49667\n" +
            "            }," +
            "            {\n" +
            "            \"name\": \"int_col\",\n" +
            "            \"type\": \"INT32\",\n" +
            "            \"charset\": 63,\n" +
            "            \"flags\": 53251\n" +
            "            }" +
            "        ],\n" +
            "        \"rows\": [" +
            "          {\n" +
            "          \"lengths\": [\"2\",\"1\"],\n" +
            "          \"values\": \"101\"\n" +
            "          }\n" +
            "        ]" +
            "      }" +
            "}";

    public static final String TEST_LAST_PKS_JSON = String.format(
            "[%s]",
            TEST_COMP_PK_LAST_PKS_JSON);

    public static final String TEST_NUMERIC_TABLE_LAST_PK_JSON = "{" +
            "      \"table_name\": \"numeric_table\",\n" +
            "      \"lastpk\": {\n" +
            "        \"fields\": [" +
            "            {\n" +
            "            \"name\": \"id\",\n" +
            "            \"type\": \"INT64\",\n" +
            "            \"charset\": 63,\n" +
            "            \"flags\": 49667\n" +
            "            }" +
            "        ],\n" +
            "        \"rows\": [" +
            "          {\n" +
            "          \"lengths\": [\"1\"],\n" +
            "          \"values\": \"5\"\n" +
            "          }\n" +
            "        ]" +
            "      }" +
            "}";

    public static final String TEST_MULTIPLE_TABLE_PKS_JSON = String.format(
            "[%s, %s]",
            TEST_COMP_PK_LAST_PKS_JSON,
            TEST_NUMERIC_TABLE_LAST_PK_JSON);

    @Test
    public void shouldCreateTablePKsFromRawTablePKs() {
        String tableName = "t1";
        Query.QueryResult queryResult = Query.QueryResult.newBuilder()
                .addFields(Query.Field.newBuilder()
                        .setName("id")
                        .setCharset(100)
                        .setFlags(200)
                        .setType(Query.Type.INT64))
                .addFields(Query.Field.newBuilder()
                        .setName("id2")
                        .setCharset(100)
                        .setFlags(200)
                        .setType(Query.Type.INT64))
                .addRows(Query.Row.newBuilder()
                        .addLengths(2)
                        .addLengths(4)
                        .setValues(ByteString.copyFrom(Ints.toByteArray(10))))
                .addRows(Query.Row.newBuilder()
                        .addLengths(2)
                        .setValues(ByteString.copyFrom(Ints.toByteArray(12))))
                .build();
        List<Binlogdata.TableLastPK> rawTableLastPK = List.of(Binlogdata.TableLastPK.newBuilder()
                .setTableName(tableName)
                .setLastpk(queryResult).build());

        TablePrimaryKeys tablePrimaryKeys = TablePrimaryKeys.createFromRawTableLastPrimaryKey(rawTableLastPK);

        assertThat(tablePrimaryKeys.getRawTableLastPrimaryKeys()).isEqualTo(rawTableLastPK);
        TablePrimaryKeys.LastPrimaryKey lastPK = new TablePrimaryKeys.LastPrimaryKey(queryResult);
        assertThat(tablePrimaryKeys.getTableLastPrimaryKeys()).containsExactly(
                new TablePrimaryKeys.TableLastPrimaryKey(tableName, lastPK)
        );
    }

    @Test
    public void shouldCreateFromLastPKs() {
        List<TablePrimaryKeys.TableLastPrimaryKey> tableLastPKs = getTestTablePKs();

        TablePrimaryKeys tablePrimaryKeys = TablePrimaryKeys.createFromTableLastPrimaryKeys(tableLastPKs);

        assertThat(tablePrimaryKeys.getRawTableLastPrimaryKeys()).isEqualTo(getTestRawTableLastPKList());
        assertThat(tablePrimaryKeys.getTableLastPrimaryKeys()).isEqualTo(getTestTablePKs());
    }

    @Test
    public void testTablePKsFromJson() {
        TablePrimaryKeys tablePrimaryKeys = TablePrimaryKeys.of(TEST_LAST_PKS_JSON);
        JSONAssert.assertEquals(tablePrimaryKeys.toString(), TEST_LAST_PKS_JSON, true);
    }
}