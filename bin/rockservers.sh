#!/usr/bin/env bash

# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   ROCKSTOR_SLAVES    File naming remote hosts.
#     Default is ${ROCKSTOR_CONF_DIR}/slaves.
#   ROCKSTOR_CONF_DIR  Alternate conf dir. Default is ${ROCKSTOR_HOME}/conf.
#   ROCKSTOR_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   ROCKSTOR_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: slaves.sh [--config confdir] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/rockstor-config.sh

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in
# rockstor-env.sh. Save it here.
HOSTLIST=$ROCKSTOR_SLAVES

if [ -f "${ROCKSTOR_CONF_DIR}/rockstor-env.sh" ]; then
  . "${ROCKSTOR_CONF_DIR}/rockstor-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$ROCKSTOR_SLAVES" = "" ]; then
    export HOSTLIST="${ROCKSTOR_CONF_DIR}/rockservers"
  else
    export HOSTLIST="${ROCKSTOR_SLAVES}"
  fi
fi

for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $ROCKSTOR_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$ROCKSTOR_SLAVE_SLEEP" != "" ]; then
   sleep $ROCKSTOR_SLAVE_SLEEP
 fi
done

wait
