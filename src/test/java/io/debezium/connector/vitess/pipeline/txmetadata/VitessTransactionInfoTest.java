/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class VitessTransactionInfoTest {

    @Test
    public void getTransactionId() {
        String expectedId = "vgtid";
        assertThat(new VitessTransactionInfo(expectedId, "shard").getTransactionId()).isEqualTo(expectedId);
    }

    @Test
    public void getShard() {
        String expectedShard = "shard";
        assertThat(new VitessTransactionInfo("vgtid", expectedShard).getShard()).isEqualTo(expectedShard);
    }
}
