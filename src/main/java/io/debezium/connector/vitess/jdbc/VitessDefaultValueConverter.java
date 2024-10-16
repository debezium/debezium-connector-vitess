/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.jdbc;

import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;

/**
 * @author Thomas Thornton
 */
public class VitessDefaultValueConverter extends BinlogDefaultValueConverter {

    public VitessDefaultValueConverter(BinlogValueConverters converters) {
        super(converters);
    }
}
