/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class VitessEpochProviderTest {

    private String prefix = "MySQL56/";
    private String host1Tx1 = "027c67a2-c0b0-11ec-8a34-0ed0087913a5:1-11418261";
    private String host1Tx2 = "027c67a2-c0b0-11ec-8a34-0ed0087913a5:1-11418262";
    private String host2Tx1 = "08fb1cf3-0ce5-11ed-b921-0a8939501751:1-1443715";

    private String previousTxId = prefix + String.join(",", host1Tx1, host2Tx1);
    private String txId = prefix + String.join(",", host1Tx2, host2Tx1);
    private String txIdShrunk = prefix + String.join(",", host1Tx2);

    private String txIdVersion5 = "MySQL57/" + String.join(",", host1Tx2);
    private String txIdVersion8 = "MySQL82/" + String.join(",", host1Tx2);

    @Test
    public void testGetEpochSameHostSet() {
        VitessEpochProvider provider = new VitessEpochProvider();
        Long epoch = provider.getEpochForGtid(0L, previousTxId, txId);
        assertThat(epoch).isEqualTo(0);
    }

    @Test
    public void testGetEpochShrunkHostSet() {
        VitessEpochProvider provider = new VitessEpochProvider();
        Long epoch = provider.getEpochForGtid(0L, previousTxId, txIdShrunk);
        assertThat(epoch).isEqualTo(1);
    }

    @Test
    public void testGetEpochExpandHostSet() {
        VitessEpochProvider provider = new VitessEpochProvider();
        Long epoch = provider.getEpochForGtid(0L, previousTxId, txId);
        assertThat(epoch).isEqualTo(0);
    }

    @Test
    public void testGetEpochDisjointThrowsException() {
        VitessEpochProvider provider = new VitessEpochProvider();
        Assertions.assertThatThrownBy(() -> {
            provider.getEpochForGtid(0L, previousTxId, "foo:1-2,bar:2-4");
        }).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testVersionUpgradeDoesNotAffectEpoch() {
        VitessEpochProvider provider = new VitessEpochProvider();
        Long epoch = provider.getEpochForGtid(0L, txIdVersion5, txIdVersion8);
        assertThat(epoch).isEqualTo(0L);
    }
}
