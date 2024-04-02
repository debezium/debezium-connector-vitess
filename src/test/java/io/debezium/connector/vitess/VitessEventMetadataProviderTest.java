/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_GTID;
import static io.debezium.connector.vitess.VgtidTest.TEST_GTID2;
import static io.debezium.connector.vitess.VgtidTest.TEST_KEYSPACE;
import static io.debezium.connector.vitess.VgtidTest.TEST_SHARD2;
import static io.debezium.connector.vitess.VgtidTest.VGTID_JSON;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessTransactionInfo;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Collect;

public class VitessEventMetadataProviderTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        final VitessConnectorConfig connectorConfig = new VitessConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "server_foo")
                        .with(VitessConnectorConfig.KEYSPACE, TEST_KEYSPACE)
                        .with(VitessConnectorConfig.SHARD, AnonymousValue.getString())
                        .with(VitessConnectorConfig.VTGATE_HOST, AnonymousValue.getString())
                        .with(VitessConnectorConfig.VTGATE_PORT, AnonymousValue.getInt())
                        .build());
        source = new SourceInfo(connectorConfig);
        source.resetVgtid(
                Vgtid.of(
                        Collect.arrayListOf(
                                new Vgtid.ShardGtid(TEST_KEYSPACE, VgtidTest.TEST_SHARD, TEST_GTID),
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2))),
                Instant.ofEpochMilli(1000));
        source.setTableId(new TableId("c", "s", "t"));
        source.setShard(VgtidTest.TEST_SHARD);
        source.setSnapshot(SnapshotRecord.FALSE);
    }

    @Test
    public void getTransactionInfo() {
        VitessSourceInfoStructMaker maker = new VitessSourceInfoStructMaker();
        maker.init("foo", "bar", new VitessConnectorConfig(TestHelper.defaultConfig().build()));
        Schema sourceSchema = maker.schema();
        Schema schema = VitessSchemaFactory.get().datatypeEnvelopeSchema()
                .withSchema(Schema.STRING_SCHEMA, "before")
                .withSchema(Schema.STRING_SCHEMA, "after")
                .withSchema(sourceSchema, "source")
                .withTransaction(VitessSchemaFactory.get().getOrderedTransactionBlockSchema())
                .build().schema();
        VitessEventMetadataProvider provider = new VitessEventMetadataProvider();
        VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        Struct valueStruct = new Struct(schema).put("source", source.struct());
        VitessTransactionInfo info = (VitessTransactionInfo) provider.getTransactionInfo(
                new TableId("foo", "bar", "foo"),
                VitessOffsetContext.initialContext(config, Clock.system()),
                null,
                valueStruct);
        assertThat(info.getShard()).isEqualTo(VgtidTest.TEST_SHARD);
        assertThat(info.getTransactionId()).isEqualTo(VGTID_JSON);
    }
}
