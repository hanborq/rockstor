ARGC=$#

function showHelp()
{
  echo "./gen_cacerts <bigdata-aaa>"
}

if [ $ARGC -ne 1 ];
then
   showHelp
   exit -1
fi

BIGDATA=$1

echo "bigdata-aaa host is : $BIGDATA"

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

export ROCKSTOR_CONSOLE_HOME

echo "Delete ssl folder : $ROCKSTOR_CONSOLE_HOME/conf/ssl"
rm -rf $ROCKSTOR_CONSOLE_HOME/conf/ssl

echo "Create ssl folder : $ROCKSTOR_CONSOLE_HOME/conf/ssl"
mkdir $ROCKSTOR_CONSOLE_HOME/conf/ssl

SERVER_KEYSTORE="$ROCKSTOR_CONSOLE_HOME/conf/ssl/bigdata.aaa.keystore"
SERVER_CRT="$ROCKSTOR_CONSOLE_HOME/conf/ssl/bigdata.aaa.crt"
SERVER_CACERTS="$ROCKSTOR_CONSOLE_HOME/conf/ssl/bigdata.aaa.cacerts"

echo "Create $SERVER_CACERTS from $BIGDATA"
java -cp "$ROCKSTOR_CONSOLE_HOME"/apache-tomcat-6.0.20/lib/bigdata-aaa-client.jar com.bigdata.aaa.util.InstallCert $BIGDATA $SERVER_CACERTS

echo "Generate $SERVER_KEYSTORE ..."
NAME="cn=$DOMAIN,ou=aaa,o=bigdata,l=sz,st=gd,c=cn"
keytool -genkey -dname "$NAME" -alias bigdata_aaa -keypass changeit -keystore $SERVER_KEYSTORE -storepass changeit

echo "Generate $SERVER_CRT ..."
keytool -export -alias bigdata_aaa -keypass changeit -file $SERVER_CRT -storepass changeit -keystore $SERVER_KEYSTORE

echo "Update $SERVER_CACERTS ..."
keytool -import -alias bigdata_aaa -keypass changeit -file $SERVER_CRT -storepass changeit -keystore $SERVER_CACERTS

