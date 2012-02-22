#!/usr/bin/env bash

# Start rockstor servers daemons.
# Optinally upgrade or rollback dfs state.
# Run this on master node.

usage="Usage: start-rockserver.sh"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/rockstor-config.sh

# start dfs daemons
# start namenode after datanodes, to minimize time namenode is up w/o data
# note: datanodes will log connection errors until namenode starts

"$bin"/rockstor-daemons.sh --config $ROCKSTOR_CONF_DIR start rockserver $rockserverStartOpt
