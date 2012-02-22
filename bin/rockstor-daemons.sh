#!/usr/bin/env bash

# Run a Rockstor command on all slave hosts.

usage="Usage: rockstor-daemons.sh [--config confdir] [--hosts hostlistfile] [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/rockstor-config.sh

exec "$bin/rockservers.sh" --config $ROCKSTOR_CONF_DIR cd "$ROCKSTOR_HOME" \; "$bin/rockstor-daemon.sh" --config $ROCKSTOR_CONF_DIR "$@"
