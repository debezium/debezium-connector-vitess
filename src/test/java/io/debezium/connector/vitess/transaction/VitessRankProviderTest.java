/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.junit.Test;

public class VitessRankProviderTest {

    @Test
    public void shouldGetRankOneHost() {
        String txId = "host1:1-4";
        RankProvider provider = new VitessRankProvider();
        BigInteger rank = provider.getRank(txId);
        assertThat(rank).isEqualTo(4);
    }

}
