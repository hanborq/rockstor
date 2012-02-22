#!/usr/bin/env bash
this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

# the root of the ROCKSTOR installation
if [ -z "$ROCKSTOR_HOME" ]; then
  export ROCKSTOR_HOME=`dirname "$this"`/..
fi

# double check that our ROCKSTOR_HOME looks reasonable.
# cding to / here verifies that we have an absolute path, which is
# necessary for the daemons to function properly
if [ -z "$(cd / && ls $ROCKSTOR_HOME/lib/rockstor*.jar $ROCKSTOR_HOME/conf 2>/dev/null)" ]; then
  cat 1>&2 <<EOF
+================================================================+
|      Error: ROCKSTOR_HOME is not set correctly                   |
+----------------------------------------------------------------+
| Please set your ROCKSTOR_HOME variable to the absolute path of   |
| the directory that contains ROCKSTOR-VERSION.jar            |
+================================================================+
EOF
  exit 1
fi

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      ROCKSTOR_CONF_DIR=$confdir
    fi
fi

# Allow alternate conf dir location.
#ROCKSTOR_CONF_DIR="${ROCKSTOR_HOME}/../share-conf"
ROCKSTOR_CONF_DIR="${ROCKSTOR_CONF_DIR:-$ROCKSTOR_HOME/conf}"

if [ -f "${ROCKSTOR_CONF_DIR}/rockstor-env.sh" ]; then
  . "${ROCKSTOR_CONF_DIR}/rockstor-env.sh"
fi


# attempt to find java
if [ -z "$JAVA_HOME" ]; then
  for candidate in \
                    "/usr/lib/jvm/java-6-sun" \
                    "/usr/lib/j2sdk1.6-sun" \
                    "/usr/java/jdk1.6*" \
                    "/usr/java/jre1.6*" \
                    "/Library/Java/Home" \
                    "/usr/java/default" \
                    "/usr/lib/jvm/default-java" ; do
    if [ -e $candidate/bin/java ]; then
      export JAVA_HOME=$candidate
      break
    fi
  done
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|       > http://java.sun.com/javase/downloads/ <                      |
|                                                                      |
| ROCKSTOR requires Java 1.6 or later.                                   |
| NOTE: This script will find Sun Java whether you install using the   |
|       binary or the RPM based installer.                             |
+======================================================================+
EOF
    exit 1
  fi
fi

if [ -d $ROCKSTOR_HOME/pids ]; then
ROCKSTOR_PID_DIR="${ROCKSTOR_PID_DIR:-$ROCKSTOR_HOME/pids}"
fi

#check to see it is specified whether to use the slaves or the
# masters file
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        slavesfile=$1
        shift
        export ROCKSTOR_SLAVES="${ROCKSTOR_CONF_DIR}/$slavesfile"
    fi
fi

