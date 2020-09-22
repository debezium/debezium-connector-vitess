#!/bin/bash

source ./env.sh

# start topo server
CELL=zone1 ./scripts/etcd-up.sh

# start vtctld
CELL=zone1 ./scripts/vtctld-up.sh

# start vttablets for keyspace commerce
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ./scripts/vttablet-up.sh
done

# set one of the replicas to master
vtctlclient InitShardMaster -force commerce/0 zone1-100

# start vtgate
CELL=zone1 ./scripts/vtgate-up.sh
