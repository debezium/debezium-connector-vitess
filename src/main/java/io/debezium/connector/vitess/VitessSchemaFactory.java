/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.schema.SchemaFactory;

public class VitessSchemaFactory extends SchemaFactory {

    public VitessSchemaFactory() {
        super();
    }

    private static final VitessSchemaFactory vitessSchemaFactoryObject = new VitessSchemaFactory();

    public static VitessSchemaFactory get() {
        return vitessSchemaFactoryObject;
    }
}
