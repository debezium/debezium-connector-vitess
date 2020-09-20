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
    protected static final String SYSTEM_SCHEMA_EXCLUDE_LIST = String.join(",", SYSTEM_SCHEMAS);

    private final Tables.TableFilter tableFilter;
    private final Tables.ColumnNameFilter columnFilter;

    /** @param config the configuration; may not be null */
    public Filters(VitessConnectorConfig config) {

        // Define the filter using the include/exclude list for table names ...
        this.tableFilter = Tables.TableFilter.fromPredicate(
                Selectors.tableSelector()
                        .includeTables(config.tableIncludeList())
                        .excludeTables(config.tableExcludeList())
                        .excludeSchemas(SYSTEM_SCHEMA_EXCLUDE_LIST)
                        .build());

        String columnIncludeList = config.columnIncludeList();
        if (columnIncludeList != null) {
            this.columnFilter = Tables.ColumnNameFilterFactory.createIncludeListFilter(columnIncludeList);
        }
        else {
            this.columnFilter = Tables.ColumnNameFilterFactory.createExcludeListFilter(config.columnExcludeList());
        }
    }

    protected Tables.TableFilter tableFilter() {
        return tableFilter;
    }

    protected Tables.ColumnNameFilter columnFilter() {
        return columnFilter;
    }
}
