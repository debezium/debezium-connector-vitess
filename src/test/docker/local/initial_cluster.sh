#!/bin/bash

# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

source ./env.sh

# start topo server
CELL=zone1 ./scripts/etcd-up.sh

# start vtctld
CELL=zone1 ./scripts/vtctld-up.sh

# start vttablets for unsharded keyspace test_unsharded_keyspace
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=test_unsharded_keyspace TABLET_UID=$i ./scripts/vttablet-up.sh
done

# set one of the replicas to primary
vtctlclient --grpc_auth_static_client_creds grpc_static_client_auth.json PlannedReparentShard -- --keyspace_shard=test_unsharded_keyspace/0 --new_primary=zone1-100

# start vttablets for sharded keyspace test_sharded_keyspace
for i in 200 201 202; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=-80 CELL=zone1 KEYSPACE=test_sharded_keyspace TABLET_UID=$i ./scripts/vttablet-up.sh
done

# set one of the replicas to primary
for i in 300 301 302; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	SHARD=80- CELL=zone1 KEYSPACE=test_sharded_keyspace TABLET_UID=$i ./scripts/vttablet-up.sh
done

vtctlclient --grpc_auth_static_client_creds grpc_static_client_auth.json PlannedReparentShard -- --keyspace_shard=test_sharded_keyspace/-80 --new_primary=zone1-200
vtctlclient --grpc_auth_static_client_creds grpc_static_client_auth.json PlannedReparentShard -- --keyspace_shard=test_sharded_keyspace/80- --new_primary=zone1-300

# create seq table unsharded keyspace, other tables and vschema in sharded keyspace
vtctlclient --grpc_auth_static_client_creds grpc_static_client_auth.json ApplySchema -- --sql-file create_tables_unsharded.sql test_unsharded_keyspace
vtctlclient --grpc_auth_static_client_creds grpc_static_client_auth.json ApplyVSchema -- --vschema_file vschema_tables_unsharded.json test_unsharded_keyspace
vtctlclient --grpc_auth_static_client_creds grpc_static_client_auth.json ApplySchema -- --sql-file create_tables_sharded.sql test_sharded_keyspace
vtctlclient --grpc_auth_static_client_creds grpc_static_client_auth.json ApplyVSchema -- --vschema_file vschema_tables_sharded.json test_sharded_keyspace

# start vtgate
CELL=zone1 ./scripts/vtgate-up.sh
