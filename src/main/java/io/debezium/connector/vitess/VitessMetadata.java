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
    private VitessConnectorConfig config;

    public VitessMetadata(VitessConnectorConfig config) {
        this.config = config;
    }

    public List<String> getShards() {
        List<String> shards;
        if (config.excludeEmptyShards()) {
            LOGGER.info("Excluding empty shards");
            shards = getVitessShardsFromTablets();
        }
        else {
            shards = getVitessShards();
        }
        LOGGER.info("Shards: {}", shards);
        return shards;
    }

    public List<String> getTables() {
        Vtgate.ExecuteResponse response;
        String query;
        if (config.excludeEmptyShards()) {
            query = "SHOW TABLES";
            List<String> shardsToQuery;
            if (config.getShard() != null && !config.getShard().isEmpty()) {
                LOGGER.info("Getting tables from one of the configured shards");
                shardsToQuery = config.getShard();
            }
            else {
                LOGGER.info("Getting tables from a non-empty shard");
                shardsToQuery = getVitessShardsFromTablets();
            }
            String randomShard = shardsToQuery.get(new Random().nextInt(shardsToQuery.size()));
            LOGGER.info("Get tables from shard: {}", randomShard);
            response = executeQuery(query, randomShard);
        }
        else {
            query = String.format("SHOW TABLES FROM %s", config.getKeyspace());
            response = executeQuery(query);
        }
        logResponse(response, query);
        List<String> tables = getFlattenedRowsFromResponse(response);
        LOGGER.info("All tables from keyspace {} are: {}", config.getKeyspace(), tables);
        return tables;
    }

    private static void logResponse(Vtgate.ExecuteResponse response, String query) {
        LOGGER.info("Got response: {} for query: {}", response, query);
    }

    private List<String> getVitessShards() {
        String query = String.format("SHOW VITESS_SHARDS LIKE '%s/%%'", config.getKeyspace());
        Vtgate.ExecuteResponse response = executeQuery(query);
        logResponse(response, query);
        List<String> rows = getFlattenedRowsFromResponse(response);
        List<String> shards = rows.stream().map(fieldValue -> {
            String[] parts = fieldValue.split("/");
            assert parts != null && parts.length == 2 : String.format("Wrong field format: %s", fieldValue);
            return parts[1];
        }).collect(Collectors.toList());
        return shards;
    }

    private List<String> getVitessShardsFromTablets() {
        String query = "SHOW VITESS_TABLETS";
        Vtgate.ExecuteResponse response = executeQuery(query);
        // Do not log the response since there is no way to filter tablets: it includes all tablets of all shards of all keyspaces
        List<List<String>> rowValues = getRowsFromResponse(response);
        List<String> shards = VitessMetadata.getNonEmptyShards(rowValues, config.getKeyspace());
        return shards;
    }

    private Vtgate.ExecuteResponse executeQuery(String query) {
        return executeQuery(query, null);
    }

    @VisibleForTesting
    protected Vtgate.ExecuteResponse executeQuery(String query, String shard) {
        // Some tests need to be issue a shard-specific query, so make this visible
        try (VitessReplicationConnection connection = new VitessReplicationConnection(config, null)) {
            Vtgate.ExecuteResponse response;
            if (shard != null) {
                response = connection.execute(query, shard);
            }
            else {
                response = connection.execute(query);
            }
            return response;
        }
        catch (Exception e) {
            throw new RuntimeException(String.format("Unexpected error while running query: %s", query), e);
        }
    }

    private static List<String> getFlattenedRowsFromResponse(Vtgate.ExecuteResponse response) {
        validateResponse(response);
        Query.QueryResult result = response.getResult();
        validateResult(result);
        List<List<String>> rows = parseRows(result.getRowsList());
        return flattenAndConcat(rows);
    }

    private static List<List<String>> getRowsFromResponse(Vtgate.ExecuteResponse response) {
        validateResponse(response);
        Query.QueryResult result = response.getResult();
        validateResult(result);
        return parseRows(result.getRowsList());
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
}
