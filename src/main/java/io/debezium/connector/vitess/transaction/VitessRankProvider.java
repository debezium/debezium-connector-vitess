/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

import java.math.BigInteger;

public class VitessRankProvider implements RankProvider {

    @Override
    public BigInteger getRank(String transactionId) {
        Gtid gtid = new Gtid(transactionId);
        BigInteger rank = new BigInteger("0");
        for (String sequenceValue : gtid.getSequenceValues()) {
            rank = rank.add(new BigInteger(sequenceValue));
        }
        return rank;
    }
}
