/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import java.math.BigDecimal;

public class VitessRankProvider {

    public BigDecimal getRank(String transactionId) {
        Gtid gtid = new Gtid(transactionId);
        BigDecimal rank = new BigDecimal("0");
        for (String sequenceValue : gtid.getSequenceValues()) {
            rank = rank.add(new BigDecimal(sequenceValue));
        }
        return rank;
    }
}
