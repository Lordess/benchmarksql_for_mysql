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

db_type=$1

if [ "$db_type" = "" ] ; then
    db_type="pg"
fi

. $BENCHMARKSQL_HOME/run/config
mkdir -p $BENCHMARKSQL_HOME/results
for SCALE in $SCALES; do
  #
  # Have a consistant, clean set of pgbench tables to start 
  # each test run with
  #
  if [ "$SKIPINIT" -ne "1" ]; then
    free -m ; sync; echo 3 >/proc/sys/vm/drop_caches ; free -m;
    #进行单次测试时，数据库如果已经有数据了，那么就忽略造数、导入的过程
    echo Creating new benchmarksql tables $SCALE from ${warehouses_initvalue}
    # warehouses_initvalue 来自环境变量
    $BENCHMARKSQL_HOME/run/runLoader.sh $db_type $SCALE $IMPORT_CONCURRENT ${warehouses_initvalue} >$BENCHMARKSQL_HOME/results/${db_type}_loader_${SCALE}-${IMPORT_CONCURRENT}.out 2>&1 
  fi
  #
  # Run the main test
  #
  for (( t=1; t<=$SETTIMES; t++ )); do
   for c in $SETCLIENTS; do
      # benchmarksql要求clients < scale * 10否则会报异常
      if [ "`expr $c \> ${SCALE}0`" = "0" ] ; then
        # 清除本地缓存
        free -m ; sync; echo 3 >/proc/sys/vm/drop_caches ; free -m;
        # 清除PG服务器的缓存，需配置ssh自动登录，否则会等待输入root登录口令
        if [ ! "$dcserver" = "" ] ; then
            echo "Drop cache $dcserver"
            ssh root@"$dcserver" "date ; free -m ; echo 3 >/proc/sys/vm/drop_caches"
        fi    
        echo Run set \#$t of $SETTIMES with $c clients scale=$SCALE
        log=$BENCHMARKSQL_HOME/results/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out
        $BENCHMARKSQL_HOME/run/runBenchmark.sh $db_type $SCALE $c $TEST_TIME_IN_MIN >$log 2>&1 
        
        echo
        grep "^STAT " $log |cut -d " " -f 7,8 >latency.txt
        gnuplot $BENCHMARKSQL_HOME/plot/latency.script 
        grep "^STAT " $log |cut -d " " -f 7|uniq -c |awk 'BEGIN{ OFS="\t"}{print $2,$1}' >tpsdata.txt
        gnuplot $BENCHMARKSQL_HOME/plot/tps.script

        mv latency.png $BENCHMARKSQL_HOME/results/latency-${db_type}_${SCALE}_${c}_${TEST_TIME_IN_MIN}.png
        mv tps.png $BENCHMARKSQL_HOME/results/tps-${db_type}_${SCALE}_${c}_${TEST_TIME_IN_MIN}.png
        rm latency.txt
        rm tpsdata.txt
      fi  
    done
  done
  if [ ! "${warehouses_initvalue}" = "" ] ; then
      warehouses_initvalue=`expr $SCALE + 1`
  fi
done
cp -rf $BENCHMARKSQL_HOME/results $BENCHMARKSQL_HOME/results_back
sh $BENCHMARKSQL_HOME/run/webreport.sh $db_type
