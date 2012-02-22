this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path.
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`

. "$bin/set_classpath.sh"

lum=`ps -eaf|grep "$ROCKSTOR_CONSOLE_HOME"/apache-tomcat-6.0.20/bin/bootstrap.jar|grep java|wc -l`

if [ $lum -eq 0 ]  
then
    echo "Rockstor Console Server is not running!"
    exit 0  
fi

"$ROCKSTOR_CONSOLE_HOME"/apache-tomcat-6.0.20/bin/catalina.sh stop
