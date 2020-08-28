/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Ignore;

public class VitessConnectorTaskTest {

    @Ignore
    public void shouldPollRecords() throws InterruptedException {
        VitessConnectorTask task = new VitessConnectorTask();
        task.initialize(dummyContext());

        Map<String, String> props = new HashMap<>();
        props.put("vitess.keyspace", "commerce");
        props.put("vitess.shard", "0");
        props.put("vitess.vtgate.host", "localhost");
        props.put("vitess.vtgate.port", "15991");
        props.put("vitess.vtctld.host", "localhost");
        props.put("vitess.vtctld.port", "15999");
        task.start(props);
        List<SourceRecord> actual = task.poll();
        assertThat(actual.size()).isGreaterThan(0);
    }

    private SourceTaskContext dummyContext() {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return null;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return null;
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                                                                                Collection<Map<String, T>> partitions) {
                        return null;
                    }
                };
            }
        };
    }
}
