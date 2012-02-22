#!/usr/bin/env bash

# Start rockstor servers daemons.
# Optinally upgrade or rollback dfs state.
# Run this on master node.

usage="Usage: start-rockserver.sh"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/rockstor-config.sh

"$bin"/rockstor-daemon.sh --config $ROCKSTOR_CONF_DIR start pflog 