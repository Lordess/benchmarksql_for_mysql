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
  [ -n "$BENCHMARKSQL_HOME" ] &&
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
factor=$2
terminals=$3
runMins=$4
#if [ "$db_type" = "" ] ; then
#    db_type="pg"
#fi
if [ "$factor" = "" ] ; then
    factor="1"
fi
SYSTEM_OPTS="-Dwarehouses=$factor"
if [ my"$terminals" != my"" ] ; then
    SYSTEM_OPTS="$SYSTEM_OPTS -Dterminals=$terminals"
fi
if [ my"$runMins" != my"" ] ; then
    SYSTEM_OPTS="$SYSTEM_OPTS -DrunMins=$runMins"
fi

if [ "$db_type" = "pg" ] ; then
    db_jdbc_jar="postgresql-9.3-1101.jdbc41.jar"
elif [ "$db_type" = "ora" ] ; then
    db_jdbc_jar="ojdbc6.jar"
elif [ "$db_type" = "mysql" ] ; then
    db_jdbc_jar="mysql-connector-java-5.1.32-bin.jar" 
elif [ "$db_type" = "mppdb" ] ; then
    db_jdbc_jar="gsjdbc4.jar"     
else	
    echo "Usage: nohup runBenchmark.sh [数据库类型] [数据量] [并发数] [持续时间]>output.txt 2>&1 &"
	echo "数据库类型: pg ora mysql mppdb，四者选一，默认为pg"
	echo "    数据量: 大于等于1的整数。数据量为10时，所占空间约1GB"
	echo "    并发数: 整数。默认从prop文件中获取，注意不能超过[数据量]的10倍"
	echo "  持续时间: 单位为分钟，整数。默认从prop文件中获取，为12分钟"
 	exit 1
fi

CP=${BENCHMARKSQL_HOME}/dist/BenchmarkSQL-4.1.jar:${BENCHMARKSQL_HOME}/lib/${db_jdbc_jar}:${BENCHMARKSQL_HOME}/lib/log4j-1.2.17.jar:${BENCHMARKSQL_HOME}/lib/apache-log4j-extras-1.1.jar:${BENCHMARKSQL_HOME}/run

echo "数据库类型: $db_type, 数据量: $factor, 并发数: $terminals, 持续时间: $runMins"
echo " JAVA_HOME: $JAVA_HOME"
echo " Classpath: $CP"
echo "      OPTS: $SYSTEM_OPTS"

#从环境变量传进来的参数，在测试之前可以执行一段sql语句（比如vacuum analyze或者其他的优化语句）
if [ ! "$tuningfirst" = "" ] ; then
   echo "Tuning first `date`"
   $JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/$tuningfirst ExecJDBC
fi

# 清除PG服务器的缓存，需配置ssh自动登录，否则会等待输入root登录口令
if [ ! "$dcserverfirst" = "" ] ; then
    echo "Drop cache $dcserverfirst"
    ssh root@"$dcserverfirst" "date ; free -m ; echo 3 >/proc/sys/vm/drop_caches"
fi

echo "$db_type 开始Benchmark_1测试..."
bm_starttime="`date`"
echo "$db_type Benchmark_1开始时间：[${bm_starttime}]"
$JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type ${SYSTEM_OPTS} jTPCC
echo "$db_type Benchmark_1: [${bm_starttime}] [`date`]"
echo ""

#测试完成之后再执行tuning
if [ ! "$tuninglast" = "" ] ; then
   echo "Tuning last `date`"
   $JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/$tuninglast ExecJDBC
fi
