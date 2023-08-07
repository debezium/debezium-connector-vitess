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
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Logical in-memory representation of Vitess schema (a.k.a Vitess keyspace). It is used to create
 * kafka connect {@link Schema} for all tables.
 */
public class VitessDatabaseSchema extends RelationalDatabaseSchema {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessDatabaseSchema.class);

    public VitessDatabaseSchema(
                                VitessConnectorConfig config,
                                SchemaNameAdjuster schemaNameAdjuster,
                                TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(
                config,
                topicNamingStrategy,
                new Filters(config).tableFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        new VitessValueConverter(
                                config.getDecimalMode(),
                                config.getTemporalPrecisionMode(),
                                ZoneOffset.UTC,
                                config.binaryHandlingMode(),
                                config.includeUnknownDatatypes(),
                                config.getBigIntUnsgnedHandlingMode()),
                        schemaNameAdjuster,
                        config.customConverterRegistry(),
                        config.getSourceInfoStructMaker().schema(),
                        config.getFieldNamer(),
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

    /**
     * Create a TableId where we use the shard name as the catalog name.
     * This is needed to ensure that schema updates are applied to each shard individually.
     * This is used in conjunction with {@link VitessTableIdToStringMapper} which excludes the shard/catalog name when
     * determining if a table is in the include/exclude list.
     * @param shard The shard of the table ID to build
     * @param keyspace The keyspace of the table ID to build
     * @param table The table name of the tbale ID to build
     * @return TableId with shard (as catalog), keyspace (as schema), and table
     */
    public static TableId buildTableId(String shard, String keyspace, String table) {
        return new TableId(shard, keyspace, table);
    }
}
