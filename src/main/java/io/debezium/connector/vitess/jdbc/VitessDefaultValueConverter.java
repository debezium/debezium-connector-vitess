/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.jdbc;

import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;

/**
 * Create a binlog default value converter to be passed into the {@link io.debezium.relational.TableSchemaBuilder}
 * in {@link io.debezium.connector.vitess.VitessDatabaseSchema}
 * @author Thomas Thornton
 */
public class VitessDefaultValueConverter extends BinlogDefaultValueConverter {

    public VitessDefaultValueConverter(BinlogValueConverters converters) {
        super(converters);
    }
}
