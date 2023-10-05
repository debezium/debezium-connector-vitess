#!/bin/bash

# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

source ./env.sh

# start topo server
CELL=zone1 ./scripts/etcd-up.sh

# start vtctld
CELL=zone1 ./scripts/vtctld-up.sh

vtctldclient CreateKeyspace --durability-policy=semi_sync test_unsharded_keyspace || fail "Failed to create and configure unsharded keyspace"
vtctldclient CreateKeyspace --durability-policy=semi_sync test_sharded_keyspace || fail "Failed to create and configure unsharded keyspace"

# start vttablets for unsharded keyspace test_unsharded_keyspace
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=test_unsharded_keyspace TABLET_UID=$i ./scripts/vttablet-up.sh
done

# start vtorc, which handles electing shard primaries (no need for InitShardPrimary manual step)
source ./scripts/vtorc-up.sh

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
wait_for_healthy_shard test_unsharded_keyspace 0 || exit 1

# start vttablets for sharded keyspace test_sharded_keyspace
for i in 200 201 202; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=-80 CELL=zone1 KEYSPACE=test_sharded_keyspace TABLET_UID=$i ./scripts/vttablet-up.sh
done

for i in 300 301 302; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=80- CELL=zone1 KEYSPACE=test_sharded_keyspace TABLET_UID=$i ./scripts/vttablet-up.sh
done

wait_for_healthy_shard test_sharded_keyspace -80 || exit 1
wait_for_healthy_shard test_sharded_keyspace 80- || exit 1

# create seq table unsharded keyspace, other tables and vschema in sharded keyspace
vtctlclient ApplySchema -- --sql-file create_tables_unsharded.sql test_unsharded_keyspace
vtctlclient ApplyVSchema -- --vschema_file vschema_tables_unsharded.json test_unsharded_keyspace
vtctlclient ApplySchema -- --sql-file create_tables_sharded.sql test_sharded_keyspace
vtctlclient ApplyVSchema -- --vschema_file vschema_tables_sharded.json test_sharded_keyspace

# start vtgate
CELL=zone1 ./scripts/vtgate-up.sh
