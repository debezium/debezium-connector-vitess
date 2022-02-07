/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Logical in-memory representation of Vitess schema (a.k.a Vitess keyspace). It is used to create
 * kafka connect {@link Schema} for all tables.
 */
public class VitessDatabaseSchema extends RelationalDatabaseSchema {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessDatabaseSchema.class);

    public VitessDatabaseSchema(
                                VitessConnectorConfig config,
                                SchemaNameAdjuster schemaNameAdjuster,
                                TopicSelector<TableId> topicSelector) {
        super(
                config,
                topicSelector,
                new Filters(config).tableFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        new VitessValueConverter(
                                config.getDecimalMode(),
                                config.getTemporalPrecisionMode(),
                                ZoneOffset.UTC,
                                config.binaryHandlingMode(),
                                config.includeUnknownDatatypes()),
                        schemaNameAdjuster,
                        config.customConverterRegistry(),
                        config.getSourceInfoStructMaker().schema(),
                        config.getSanitizeFieldNames(),
                        false),
                false,
                config.getKeyMapper());
    }

    /** Applies schema changes for the specified table. */
    public void applySchemaChangesForTable(Table table) {

        if (isFilteredOut(table.id())) {
            LOGGER.trace("Skipping schema refresh for table '{}' as table is filtered", table.id());
            return;
        }

        refresh(table);
    }

    private boolean isFilteredOut(TableId id) {
        return !getTableFilter().isIncluded(id);
    }

    /** Refreshes the schema content with a table constructed externally */
    @Override
    public void refresh(Table table) {
        tables().overwriteTable(table);
        refreshSchema(table.id());
    }

    @Override
    public void refreshSchema(TableId id) {
        LOGGER.trace("refreshing DB schema for table '{}'", id);
        Table table = tableFor(id);

        buildAndRegisterSchema(table);
    }

    public static TableId parse(String table) {
        return TableId.parse(table, false);
    }
}
