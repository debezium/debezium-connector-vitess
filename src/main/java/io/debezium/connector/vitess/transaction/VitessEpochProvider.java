/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VitessEpochProvider implements EpochProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessEpochProvider.class);

    @Override
    public Long getEpoch(Long previousEpoch, String previousTransactionId, String transactionId) {
        if (previousTransactionId == null) {
            return 0L;
        }

        Gtid previousGtid = new Gtid(previousTransactionId);
        Gtid gtid = new Gtid(transactionId);
        if (previousGtid.isHostSetEqual(gtid) || gtid.isHostSetSupersetOf(previousGtid)) {
            return previousEpoch;
        }
        else if (gtid.isHostSetSubsetOf(previousGtid)) {
            return previousEpoch + 1;
        }
        else {
            LOGGER.error(
                    "Error determining epoch, previous host set: {}, host set: {}",
                    previousGtid, gtid);
            throw new RuntimeException("Can't determine epoch");
        }
    }
}
