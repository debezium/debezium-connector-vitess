/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

public class VitessRankProviderTest {

    @Test
    public void shouldGetRankOneHost() {
        String txId = "host1:1-4";
        BigDecimal rank = VitessRankProvider.getRank(txId);
        assertThat(rank).isEqualTo(new BigDecimal(4));
    }

}
