/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Arrays;
import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/** A utility that contains various filters for acceptable {@link TableId}s and columns. */
@Immutable
public class Filters {

    protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("mysql", "performance_schema", "sys", "information_schema");
    protected static final String SYSTEM_SCHEMA_BLACKLIST = String.join(",", SYSTEM_SCHEMAS);

    private final Tables.TableFilter tableFilter;
    private final Tables.ColumnNameFilter columnFilter;

    /** @param config the configuration; may not be null */
    public Filters(VitessConnectorConfig config) {

        // Define the filter using the whitelists and blacklists for table names ...
        this.tableFilter = Tables.TableFilter.fromPredicate(
                Selectors.tableSelector()
                        .includeTables(config.tableWhitelist())
                        .excludeTables(config.tableBlacklist())
                        .excludeSchemas(SYSTEM_SCHEMA_BLACKLIST)
                        .build());

        String columnWhitelist = config.columnWhitelist();
        if (columnWhitelist != null) {
            this.columnFilter = Tables.ColumnNameFilterFactory.createWhitelistFilter(config.columnWhitelist());
        }
        else {
            this.columnFilter = Tables.ColumnNameFilterFactory.createBlacklistFilter(config.columnBlacklist());
        }
    }

    protected Tables.TableFilter tableFilter() {
        return tableFilter;
    }

    protected Tables.ColumnNameFilter columnFilter() {
        return columnFilter;
    }
}
