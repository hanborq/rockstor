# Set ROCKSTOR-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/j2sdk1.6-sun
export JAVA_HOME=/usr/java/latest

# Extra Java CLASSPATH elements.  Optional.
# export ROCKSTOR_CLASSPATH="<extra_entries>:$ROCKSTOR_CLASSPATH"
export ROCKSTOR_CLASSPATH=$ROCKSTOR_HOME/conf

# The maximum amount of heap to use, in MB. Default is 1000.
# export ROCKSTOR_HEAPSIZE=2000
export ROCKSTOR_HEAPSIZE=1000

# Extra Java runtime options.  Empty by default.
# if [ "$ROCKSTOR_OPTS" == "" ]; then export ROCKSTOR_OPTS=-server; else ROCKSTOR_OPTS+=" -server"; fi
if [ "$ROCKSTOR_OPTS" == "" ]; then
  export ROCKSTOR_OPTS="-server -XX:+UseConcMarkSweepGC";
else
  ROCKSTOR_OPTS+=" -server -XX:+UseConcMarkSweepGC";
fi

# Command specific options appended to ROCKSTOR_OPTS when specified
export ROCKSTOR_ROCKSERVER_OPTS="-Dlog.home=$ROCKSTOR_HOME/logs \
								 -Dcom.sun.management.jmxremote \
								 -Dcom.sun.management.jmxremote.port=6888 \
								 -Dcom.sun.management.jmxremote.ssl=false \
								 -Dcom.sun.management.jmxremote.authenticate=false \
								 -Dlog4j.configuration=file:$ROCKSTOR_HOME/conf/log4j_rockserver.properties \
-javaagent:$ROCKSTOR_HOME/lib/jmxetric-0.0.4.jar=config=$ROCKSTOR_HOME/conf/jmxetric_rockserver.xml"

export ROCKSTOR_MASTER_OPTS="-Dcom.sun.management.jmxremote $ROCKSTOR_MASTER_OPTS"

# export ROCKSTOR_TASKTRACKER_OPTS=
# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
# export ROCKSTOR_CLIENT_OPTS

# Extra ssh options.  Empty by default.
# export ROCKSTOR_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=ROCKSTOR_CONF_DIR"

# Parent dir for logs and pids
export ROCKSTOR_TMP_DIR=${ROCKSTOR_HOME}

# Where log files are stored.  $ROCKSTOR_HOME/logs by default.
export ROCKSTOR_LOG_DIR=${ROCKSTOR_TMP_DIR}/logs

# File naming remote rockserver hosts.  $ROCKSTOR_HOME/conf/rockservers by default.
# export ROCKSTOR_SLAVES=${ROCKSTOR_HOME}/conf/rockservers

# host:path where ROCKSTOR code should be rsync'd from.  Unset by default.
# export ROCKSTOR_MASTER=master:/home/$USER/src/rockstor

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export ROCKSTOR_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
export ROCKSTOR_PID_DIR=${ROCKSTOR_TMP_DIR}/pids

# A string representing this instance of ROCKSTOR. $USER by default.
# export ROCKSTOR_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export ROCKSTOR_NICENESS=10
