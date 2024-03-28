/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import io.debezium.pipeline.txmetadata.TransactionInfo;

public class VitessTransactionInfo implements TransactionInfo {

    private final String vgtid;
    private final String shard;

    public VitessTransactionInfo(String vgtid, String shard) {
        this.vgtid = vgtid;
        this.shard = shard;
    }

    @Override
    public String getTransactionId() {
        return this.vgtid;
    }

    public String getShard() {
        return this.shard;
    }
}
