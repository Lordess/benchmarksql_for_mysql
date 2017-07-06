#! /bin/sh

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
mingw=false;
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true
           if [ -z "$JAVA_HOME" ] ; then
             JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Home
           fi
           ;;
  MINGW*) mingw=true ;;
esac

if [ -z "$BENCHMARKSQL_HOME" -o ! -d "$BENCHMARKSQL_HOME" ] ; then
  ## resolve links - $0 may be a link to ant's home
  PRG="$0"
  progname=`basename "$0"`

  # need this for relative symlinks
  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
    else
    PRG=`dirname "$PRG"`"/$link"
    fi
  done

  BENCHMARKSQL_HOME=`dirname "$PRG"`/..

  # make it fully qualified
  BENCHMARKSQL_HOME=`cd "$BENCHMARKSQL_HOME" > /dev/null && pwd`
fi

# For Cygwin and Mingw, ensure paths are in UNIX format before
# anything is touched
if $cygwin ; then
  [ -n "$BENCHMARKSQL_HOME" ] &&
    BENCHMARKSQL_HOME=`cygpath --unix "$BENCHMARKSQL_HOME"`
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
fi
if $mingw ; then
  [ -n "$ANT_HOME" ] &&
    BENCHMARKSQL_HOME="`(cd "$BENCHMARKSQL_HOME"; pwd)`"
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME="`(cd "$JAVA_HOME"; pwd)`"
fi

JAVACMD=$JAVA_HOME/bin/java

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute $JAVACMD"
  exit 1
fi



db_type=$1

#if [ "$db_type" = "" ] ; then
#    db_type="pg"
#fi


if [ "$db_type" = "pg" ] ; then
    db_jdbc_jar="postgresql-9.3-1101.jdbc41.jar"
elif [ "$db_type" = "ora" ] ; then
    db_jdbc_jar="ojdbc6.jar"
elif [ "$db_type" = "mysql" ] ; then
    db_jdbc_jar="mysql-connector-java-5.1.32-bin.jar" 
elif [ "$db_type" = "mppdb" ] ; then
    db_jdbc_jar="gsjdbc4.jar"     
else	
    echo "Usage: nohup runSQL.sh [数据库类型]  >output.txt 2>&1 &"
	echo "数据库类型: pg ora mysql mppdb，四者选一"
 	exit 1
fi

CP=${BENCHMARKSQL_HOME}/dist/BenchmarkSQL-4.1.jar:${BENCHMARKSQL_HOME}/lib/${db_jdbc_jar}:${BENCHMARKSQL_HOME}/lib/log4j-1.2.17.jar:${BENCHMARKSQL_HOME}/lib/apache-log4j-extras-1.1.jar:${BENCHMARKSQL_HOME}/run

echo "数据库类型: $db_type"
echo " JAVA_HOME: $JAVA_HOME"
echo " Classpath: $CP"




echo "$db_type 开始建表..."
echo "$db_type Benchmark_Createtable开始时间：[`date`]"
$JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/sqlTableDrops   ExecJDBC
$JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/sqlTableCreates ExecJDBC
echo "$db_type Benchmark_Createtable完成时间：[`date`]"
echo ""






