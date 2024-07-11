/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.vitess.proto.Query;
import io.vitess.proto.Vtgate;

/**
 * Class for getting metadata on Vitess, e.g., tables, shards. Supports shard-specific queries.
 */
public class VitessMetadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessMetadata.class);

    public static List<String> getShards(VitessConnectorConfig config) {
        List<String> shards;
        if (config.excludeEmptyShards()) {
            LOGGER.info("Excluding empty shards from shard list");
            shards = getVitessShardsFromTablets(config);
        }
        else {
            shards = getVitessShards(config);
        }
        LOGGER.info("Shards: {}", shards);
        return shards;
    }

    public static List<String> getTables(VitessConnectorConfig config) {
        List<String> tables;
        if (config.excludeEmptyShards()) {
            String query = String.format("SHOW TABLES", config.getKeyspace());
            List<String> shardsToQuery;
            List<String> nonEmptyShards = getVitessShardsFromTablets(config);
            // If there is a shard list specified, then query one of its non-empty shards
            if (config.getShard() != null && !config.getShard().isEmpty()) {
                List<String> shardList = config.getShard();
                shardsToQuery = intersect(shardList, nonEmptyShards);
            }
            else {
                shardsToQuery = nonEmptyShards;
            }
            String randomNonEmptyShard = shardsToQuery.get(new Random().nextInt(shardsToQuery.size()));
            LOGGER.info("Get table list from non-empty shard: {}", randomNonEmptyShard);
            tables = flattenAndConcat(getRowsFromQuery(config, query, randomNonEmptyShard));
        }
        else {
            String query = String.format("SHOW TABLES FROM %s", config.getKeyspace());
            tables = flattenAndConcat(getRowsFromQuery(config, query));
        }
        LOGGER.info("All tables from keyspace {} are: {}", config.getKeyspace(), tables);
        return tables;
    }

    private static List<String> getVitessShards(VitessConnectorConfig config) {
        String query = String.format("SHOW VITESS_SHARDS LIKE '%s/%%'", config.getKeyspace());
        List<String> rows = flattenAndConcat(getRowsFromQuery(config, query));
        List<String> shards = rows.stream().map(fieldValue -> {
            String[] parts = fieldValue.split("/");
            assert parts != null && parts.length == 2 : String.format("Wrong field format: %s", fieldValue);
            return parts[1];
        }).collect(Collectors.toList());
        return shards;
    }

    private static List<String> getVitessShardsFromTablets(VitessConnectorConfig config) {
        String query = "SHOW VITESS_TABLETS";
        List<List<String>> rowValues = getRowsFromQuery(config, query);
        List<String> shards = VitessMetadata.getNonEmptyShards(rowValues, config.getKeyspace());
        return shards;
    }

    private static Vtgate.ExecuteResponse executeQuery(VitessConnectorConfig config, String query) {
        return executeQuery(config, query, null);
    }

    @VisibleForTesting
    protected static Vtgate.ExecuteResponse executeQuery(VitessConnectorConfig config, String query, String shard) {
        // Some tests need to be issue a shard-specific query, so make this visible
        try (VitessReplicationConnection connection = new VitessReplicationConnection(config, null)) {
            Vtgate.ExecuteResponse response;
            if (shard != null) {
                response = connection.execute(query, shard);
            }
            else {
                response = connection.execute(query);
            }
            LOGGER.info("Got response: {} for query: {}", response, query);
            return response;
        }
        catch (Exception e) {
            throw new RuntimeException(String.format("Unexpected error while running query: %s", query), e);
        }
    }

    private static List<List<String>> getRowsFromResponse(Vtgate.ExecuteResponse response) {
        validateResponse(response);
        Query.QueryResult result = response.getResult();
        validateResult(result);
        return parseRows(result.getRowsList());
    }

    private static List<List<String>> getRowsFromQuery(VitessConnectorConfig config, String query) {
        Vtgate.ExecuteResponse response = executeQuery(config, query);
        return getRowsFromResponse(response);
    }

    private static List<List<String>> getRowsFromQuery(VitessConnectorConfig config, String query, String shard) {
        Vtgate.ExecuteResponse response = executeQuery(config, query, shard);
        return getRowsFromResponse(response);
    }

    private static void validateResponse(Vtgate.ExecuteResponse response) {
        assert response != null && !response.hasError() && response.hasResult()
                : String.format("Error response: %s", response);
    }

    private static void validateResult(Query.QueryResult result) {
        List<Query.Row> rows = result.getRowsList();
        assert !rows.isEmpty() : String.format("Empty response: %s", result);
    }

    @VisibleForTesting
    protected static List<List<String>> parseRows(List<Query.Row> rows) {
        List<List<String>> allRowValues = new ArrayList<>();
        for (Query.Row row : rows) {
            List<String> currentRowValues = new ArrayList();
            List<Integer> lengths = row.getLengthsList().stream().map(x -> toIntExact(x)).collect(Collectors.toList());
            ByteString values = row.getValues();

            int offset = 0;
            for (int length : lengths) {
                if (length == -1) {
                    currentRowValues.add(null); // Handle NULL values
                }
                else {
                    String value = values.substring(offset, offset + length).toStringUtf8();
                    currentRowValues.add(value);
                    offset += length;
                }
            }
            allRowValues.add(currentRowValues);
        }
        return allRowValues;
    }

    @VisibleForTesting
    protected static List<String> getNonEmptyShards(List<List<String>> vitessTabletRows, String keyspace) {
        Set<String> shardSet = new HashSet<>();

        for (List<String> row : vitessTabletRows) {
            if (row.size() < 3) {
                continue; // skip rows with insufficient data
            }
            String rowKeyspace = row.get(1);
            if (rowKeyspace.equals(keyspace)) {
                shardSet.add(row.get(2)); // add the shard value
            }
        }

        return shardSet.stream().sorted().collect(Collectors.toList());
    }

    @VisibleForTesting
    protected static List<String> flattenAndConcat(List<List<String>> nestedList) {
        return nestedList.stream()
                .map(innerList -> String.join("", innerList))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected static List<String> intersect(List<String> list1, List<String> list2) {
        List<String> intersection = new ArrayList<>(list1);
        intersection.retainAll(list2);
        return intersection;
    }

}
