#!/usr/bin/env bash
# Stop rockserver daemons.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/rockstor-config.sh

"$bin"/rockstor-daemon.sh --config $ROCKSTOR_CONF_DIR stop pflog