/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transforms;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.vitess.SourceInfo;
import io.debezium.converters.spi.RecordParser;
import io.debezium.transforms.spi.QualifiedDataCollectionNameResolver;

public class VitessQualifiedDataCollectionNameResolver extends VitessAbstractRecordParserProvider implements QualifiedDataCollectionNameResolver {
    public static final String FULLY_QUALIFIED_NAME_FORMAT = "%s.%s";

    @Override
    public String resolve(RecordParser recordParser) {
        return String.format(FULLY_QUALIFIED_NAME_FORMAT,
                recordParser.getMetadata(SourceInfo.KEYSPACE_NAME_KEY),
                recordParser.getMetadata(AbstractSourceInfo.TABLE_NAME_KEY));
    }
}
