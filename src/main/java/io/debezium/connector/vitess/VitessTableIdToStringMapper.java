/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.TableId;

public class VitessTableIdToStringMapper implements TableIdToStringMapper {

    /**
     * Convert a table ID to the string used for table filtering.
     *
     * Since TableIDs include the shard as the catalog, the returned string used for include/exclude must exclude
     * this shard such that we apply inclusion & exclusion logic uniformly across all shards.
     * @param tableId {@link TableId} to convert to string
     * @return String representation of the table
     */
    @Override
    public String toString(TableId tableId) {
        return tableId.schema() + "." + tableId.table();
    }

}
