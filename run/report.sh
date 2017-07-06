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
 
db_type=$1
 
if [ "$db_type" = "" ] ; then
    db_type="ora"
fi
 
. $BENCHMARKSQL_HOME/run/config
 
for SCALE in $SCALES; do
  #
  # Run the main test
  #
 
  for (( t=1; t<=$SETTIMES; t++ )); do
    for c in $SETCLIENTS; do
      # benchmarksqlҪÇclients < scale * 10·ñᱨÒ³£
      if [ "`expr $c \> ${SCALE}0`" = "0" ] ; then
        echo Run set \#$t of $SETTIMES with $c clients scale=$SCALE
        log=$BENCHMARKSQL_HOME/results/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out
        echo
        grep "^STAT " $log |cut -d " " -f 7,8 >latency.txt
        gnuplot $BENCHMARKSQL_HOME/plot/latency.script 
        grep "^STAT " $log |cut -d " " -f 7|uniq -c |awk 'BEGIN{ OFS="\t"}{print $2,$1}' >tpsdata.txt
        gnuplot $BENCHMARKSQL_HOME/plot/tps.script
 
        mv latency.png $BENCHMARKSQL_HOME/results/latency-${db_type}_${SCALE}_${c}_${TEST_TIME_IN_MIN}.png
        mv tps.png $BENCHMARKSQL_HOME/results/tps-${db_type}_${SCALE}_${c}_${TEST_TIME_IN_MIN}.png
        mv latency.txt $BENCHMARKSQL_HOME/results/latency-${db_type}_${SCALE}_${c}_${TEST_TIME_IN_MIN}.txt
        mv tpsdata.txt $BENCHMARKSQL_HOME/results/tpsdata-${db_type}_${SCALE}_${c}_${TEST_TIME_IN_MIN}.txt
      fi  
    done
  done
done
$BENCHMARKSQL_HOME/run/webreport.sh $db_type
