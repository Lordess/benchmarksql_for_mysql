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
factor=$2
parallella=$3
warehouses_initvalue=$4
#if [ "$db_type" = "" ] ; then
#    db_type="pg"
#fi
if [ "$factor" = "" ] ; then
    factor="1"
fi
if [ "$parallella" = "" ] ; then
    parallella="1"
fi
if [ "${warehouses_initvalue}" = "" ] ; then
    warehouses_initvalue="1"
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
    echo "Usage: nohup runLoader.sh [数据库类型] [数据量] [并行度] [仓库开始位置]>output.txt 2>&1 &"
	echo "数据库类型: pg ora mysql mppdb，四者选一"
	echo "    数据量: 大于等于1的整数。数据量为10时，所占空间约1GB"
	echo "    并行度: 大于等于1的整数。"
  echo "仓库开始位置: 如果不为1，则保留原有仓库数据，从指定位置开始造数；否则清空已有数据，重新造数"
 	exit 1
fi

CP=${BENCHMARKSQL_HOME}/dist/BenchmarkSQL-4.1.jar:${BENCHMARKSQL_HOME}/lib/${db_jdbc_jar}:${BENCHMARKSQL_HOME}/lib/log4j-1.2.17.jar:${BENCHMARKSQL_HOME}/lib/apache-log4j-extras-1.1.jar:${BENCHMARKSQL_HOME}/run

echo "数据库类型: $db_type, 数据量: $factor, 并行度: $parallella，仓库开始位置：${warehouses_initvalue}"
echo " JAVA_HOME: $JAVA_HOME"
echo " Classpath: $CP"


#if [ "${warehouses_initvalue}" = "1" ] ; then
#  echo "$db_type 开始建表..."
#  echo "$db_type Benchmark_Createtable开始时间：[`date`]"
#  $JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/sqlTableDrops   ExecJDBC
#  $JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/sqlTableCreates ExecJDBC
#  echo "$db_type Benchmark_Createtable完成时间：[`date`]"
#  echo ""
#else
#  echo "$db_type 开始删除索引..."
#  bm_starttime="`date`"
#  echo "$db_type Drop_Index开始时间：[${bm_starttime}]"
#  $JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type  -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/sqlIndexDrops ExecJDBC
#  echo "$db_type Benchmark_I: [${bm_starttime}] [`date`]"
#  echo ""
#fi

echo "$db_type 开始清除数据[from ${warehouses_initvalue}]..."
bm_starttime="`date`"
echo "$db_type Benchmark_Truncate开始时间：[${bm_starttime}]"
$JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type  -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/sqlTableTruncates ExecJDBC
echo "$db_type Benchmark_Truncate: [${bm_starttime}] [`date`]"
echo ""



echo "$db_type 开始导入数据[from ${warehouses_initvalue}]..."
bm_starttime="`date`"
echo "$db_type Benchmark_Loaddata开始时间：[${bm_starttime}]"
$JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.load.$db_type LoadData numWarehouses $factor parallella $parallella warehouses_initvalue ${warehouses_initvalue}
echo "$db_type Benchmark_L: [${bm_starttime}] [`date`]"
echo ""



#echo "$db_type 开始建索引..."
#bm_starttime="`date`"
#echo "$db_type Benchmark_Index开始时间：[${bm_starttime}]"
#$JAVACMD -cp $CP -Dprop=${BENCHMARKSQL_HOME}/run/props.$db_type  -DcommandFile=${BENCHMARKSQL_HOME}/run/sql_$db_type/sqlIndexCreates ExecJDBC
#echo "$db_type Benchmark_I: [${bm_starttime}] [`date`]"
#echo ""
