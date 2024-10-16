/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import org.junit.After;

/**
 * @author Thomas Thornton
 */
public class VitessTestCleanup {

    public VitessDatabaseSchema schema;
    public VitessConnectorTask task;

    @After
    public void afterEach() {
        if (schema != null) {
            try {
                schema.close();
            }
            finally {
                schema = null;
            }
        }
        if (task != null) {
            try {
                task.doStop();
            }
            finally {
                task = null;
            }
        }
    }

}
