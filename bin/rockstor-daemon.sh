#!/usr/bin/env bash

# Runs a RockStor command as a daemon.
#
# Environment Variables
#
#   ROCKSTOR_CONF_DIR  Alternate conf dir. Default is ${ROCKSTOR_HOME}/conf.
#   ROCKSTOR_LOG_DIR   Where log files are stored.  PWD by default.
#   ROCKSTOR_MASTER    host:path where rockstor code should be rsync'd from
#   ROCKSTOR_PID_DIR   The pid files are stored. /tmp by default.
#   ROCKSTOR_IDENT_STRING   A string representing this instance of rockstor. $USER by default
#   ROCKSTOR_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: rockstor-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <rockstor-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/rockstor-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

rockstor_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$_ROCKSTOR_DAEMON_OUT" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$_ROCKSTOR_DAEMON_OUT.$prev" ] && mv "$_ROCKSTOR_DAEMON_OUT.$prev" "$_ROCKSTOR_DAEMON_OUT.$num"
	    num=$prev
	done
	mv "$_ROCKSTOR_DAEMON_OUT" "$_ROCKSTOR_DAEMON_OUT.$num";
    fi
}

if [ -f "${ROCKSTOR_CONF_DIR}/rockstor-env.sh" ]; then
  . "${ROCKSTOR_CONF_DIR}/rockstor-env.sh"
fi

if [ "$ROCKSTOR_IDENT_STRING" = "" ]; then
  export ROCKSTOR_IDENT_STRING="$USER"
fi

# get log directory
if [ "$ROCKSTOR_LOG_DIR" = "" ]; then
  export ROCKSTOR_LOG_DIR="$ROCKSTOR_HOME/logs"
fi
mkdir -p "$ROCKSTOR_LOG_DIR"

if [ "$ROCKSTOR_PID_DIR" = "" ]; then
  ROCKSTOR_PID_DIR=/tmp
fi

# some variables
export ROCKSTOR_LOGFILE=rockstor-$ROCKSTOR_IDENT_STRING-$command-$HOSTNAME.log
export ROCKSTOR_ROOT_LOGGER="INFO,DRFA"
export ROCKSTOR_SECURITY_LOGGER="INFO,DRFAS"
export _ROCKSTOR_DAEMON_OUT=$ROCKSTOR_LOG_DIR/rockstor-$ROCKSTOR_IDENT_STRING-$command-$HOSTNAME.out
export _ROCKSTOR_DAEMON_PIDFILE=$ROCKSTOR_PID_DIR/rockstor-$ROCKSTOR_IDENT_STRING-$command.pid
export _ROCKSTOR_DAEMON_DETACHED="true"

# Set default scheduling priority
if [ "$ROCKSTOR_NICENESS" = "" ]; then
    export ROCKSTOR_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$ROCKSTOR_PID_DIR"

    if [ -f $_ROCKSTOR_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_ROCKSTOR_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo $command running as process `cat $_ROCKSTOR_DAEMON_PIDFILE`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$ROCKSTOR_MASTER" != "" ]; then
      echo rsync from $ROCKSTOR_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $ROCKSTOR_MASTER/ "$ROCKSTOR_HOME"
    fi

    rockstor_rotate_log $_ROCKSTOR_DAEMON_OUT
    echo starting $command, logging to $_ROCKSTOR_DAEMON_OUT
    cd "$ROCKSTOR_HOME"

    nice -n $ROCKSTOR_NICENESS "$ROCKSTOR_HOME"/bin/rockstor --config $ROCKSTOR_CONF_DIR $command "$@" < /dev/null
    ;;

  (stop)

    if [ -f $_ROCKSTOR_DAEMON_PIDFILE ]; then
      if kill -0 `cat $_ROCKSTOR_DAEMON_PIDFILE` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $_ROCKSTOR_DAEMON_PIDFILE`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


