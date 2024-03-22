/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

interface EpochProvider {
    Long getEpoch(Long previousEpoch, String previousTransactionId, String transactionId);

}
