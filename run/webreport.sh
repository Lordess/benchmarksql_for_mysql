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
    db_type="pg"
fi

. $BENCHMARKSQL_HOME/run/config

if [ "$results_dir" = "" ] ; then
    results_dir="results"
fi

OUTFILE="$BENCHMARKSQL_HOME/${results_dir}/index_${db_type}.html"
echo > $OUTFILE
echo "<img src=\"scaling_${db_type}.png\"><p>" >> $OUTFILE
echo "<img src=\"clients_${db_type}.png\"><p>" >> $OUTFILE
echo "<img src=\"3d_${db_type}.png\"><p>" >> $OUTFILE

OUTFILE_MD="$BENCHMARKSQL_HOME/${results_dir}/index_${db_type}.markdown"
echo >$OUTFILE_MD
echo "![scaling](scaling_${db_type}.png)" >> $OUTFILE_MD
echo "![clients](clients_${db_type}.png)" >> $OUTFILE_MD
echo "![3d](3d_${db_type}.png)" >> $OUTFILE_MD


echo >>$OUTFILE_MD
echo "### Averages for test set $SET by scale"
echo "<h1> Averages for test set $SET by scale: </h1>">> $OUTFILE
echo "<table  border=\"1\">">>$OUTFILE
echo "<tr><td>数据规模 </td><td>导入（行/秒）</td><td> 性能指标（tpmC）</td><td> 平均延时 </td><td> 延时90%< </td><td> 最大延时 </td></tr>">>$OUTFILE

echo "### Averages for test set $SET by scale: ">> $OUTFILE_MD
echo >>$OUTFILE_MD
echo "数据规模 | 导入（行/秒） | 性能指标（tpmC）| 平均延时 | 延时90%< | 最大延时 ">>$OUTFILE_MD
echo ":--------|:--------------|:----------------|:---------|:---------|:---------" >>$OUTFILE_MD

# 最后生成统计图形
echo > scaling.txt
for SCALE in $SCALES; do
    #如果相关日志未生成，则直接忽略之后的过程
    #这种怪异的判断方法来自http://stackoverflow.com/questions/6363441/check-if-a-file-exists-with-wildcard-in-shell-script 
    files=($BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out)
    if [ -e "${files[0]}"  ] ; then
      echo "### Averages for test set $SET by scale: $SCALE"
      #grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|cut -d " " -f 1,11 |tr "_" " " | cut -d " " -f 3,6 |awk '{print $1,$1*76.8,$2}' >>scaling.txt
      
      tpmc_total=`grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|cut -d " " -f 11 |awk '{sum += $1};END {print sum}' `
      tpmc_cnt=`grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|cut -d " " -f 11 |wc -l `
      tpmc=`echo $tpmc_total $tpmc_cnt|awk '{print $1/$2}'`      

      if [ ! "$tpmc" = "" ] ; then  
        
        TOTAL_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |awk '{sum += $1};END {print sum}'`
        MIN_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n|head -n 1`
        COUNT=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|wc -l`
        
        COUNT_90PERCENT=`echo $COUNT| awk '{printf("%d\n", $1 * 10 / 100);}'`
       
        LATENCY_AVG_IN_MS=`echo $TOTAL_IN_MS $COUNT| awk '{print $1 / $2 }'`
        LATENCY_90PERCENT_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n |tail -n $COUNT_90PERCENT |head -n 1`
        LATENCY_MAX_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_*_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n|tail -n 1`

        if [ -f "$BENCHMARKSQL_HOME/${results_dir}/${db_type}_loader_${SCALE}-${IMPORT_CONCURRENT}.out" ] ; then
          imprps=`grep "Rows Per Second" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_loader_${SCALE}-${IMPORT_CONCURRENT}.out|cut -d " " -f 5`
        fi  
        echo ${SCALE} $tpmc $LATENCY_AVG_IN_MS $LATENCY_90PERCENT_IN_MS  |awk '{print $1,$1*76.8,$2,$3,$4}' >>scaling.txt
        echo "<tr><td>" ${SCALE} "</td><td>" $imprps "</td><td>" $tpmc "</td><td>" $LATENCY_AVG_IN_MS "</td><td>" $LATENCY_90PERCENT_IN_MS "</td><td>" $LATENCY_MAX_IN_MS "</td></tr>" >>$OUTFILE
        echo ${SCALE} "|" $imprps "|" $tpmc "|" $LATENCY_AVG_IN_MS "|" $LATENCY_90PERCENT_IN_MS "|" $LATENCY_MAX_IN_MS >>$OUTFILE_MD
      fi
    fi  
done	
echo "</table>" >>$OUTFILE
echo >>$OUTFILE_MD

if [ ! "`cat scaling.txt|wc -c`" = "1" ] ; then
  gnuplot $BENCHMARKSQL_HOME/plot/scaling.plot
  mv scaling.png $BENCHMARKSQL_HOME/${results_dir}/scaling_${db_type}.png
  mv scaling.txt $BENCHMARKSQL_HOME/${results_dir}/scaling_${db_type}-1.txt
  #rm scaling.txt
fi

echo "### Averages for test set $SET by clients"
echo "<h1> Averages for test set $SET by clients: </h1>">> $OUTFILE
echo "<table  border=\"1\">">>$OUTFILE
echo "<tr><td>并发数 </td><td> 性能指标（tpmC）</td><td> 平均延时 </td><td> 延时90%< </td><td> 最大延时 </td></tr>">>$OUTFILE

echo "### Averages for test set $SET by clients: ">> $OUTFILE_MD
echo >>$OUTFILE_MD
echo "并发数 | 性能指标（tpmC）| 平均延时 | 延时90%< | 最大延时 ">>$OUTFILE_MD
echo ":------|:----------------|:---------|:---------|:---------" >>$OUTFILE_MD

echo > clients.txt
for c in $SETCLIENTS; do

    files=($BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out)
    if [ -e "${files[0]}" ] ; then
      echo "### Averages for test set $SET by clients: $c"

      #grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 1,11 |tr "_" " " | cut -d " " -f 4,6 >>clients.txt

      tpmc_total=`grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 11 |awk '{sum += $1};END {print sum}' `
      tpmc_cnt=`grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 11 |wc -l `
      tpmc=`echo $tpmc_total $tpmc_cnt|awk '{print $1/$2}'`

      if [ ! "$tpmc" = "" ] ; then  
        echo ${c} $tpmc >>clients.txt
        TOTAL_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |awk '{sum += $1};END {print sum}'`
        MIN_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n|head -n 1`
        COUNT=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|wc -l`
        COUNT_90PERCENT=`echo $COUNT| awk '{printf("%d\n", $1 * 10 / 100);}'`
       
        LATENCY_AVG_IN_MS=`echo $TOTAL_IN_MS $COUNT| awk '{print $1 / $2 }'`
        LATENCY_90PERCENT_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n |tail -n $COUNT_90PERCENT |head -n 1`
        LATENCY_MAX_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_*_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n|tail -n 1`

        echo "<tr><td>" ${c} "</td><td>" $tpmc "</td><td>" $LATENCY_AVG_IN_MS "</td><td>" $LATENCY_90PERCENT_IN_MS "</td><td>" $LATENCY_MAX_IN_MS "</td></tr>" >>$OUTFILE
        echo ${c} "|" $tpmc "|" $LATENCY_AVG_IN_MS "|" $LATENCY_90PERCENT_IN_MS "|" $LATENCY_MAX_IN_MS >>$OUTFILE_MD
      fi
    fi  
done	
echo "</table>" >>$OUTFILE
echo >>$OUTFILE_MD

if [ ! "`cat clients.txt|wc -c`" = "1" ] ; then
  gnuplot $BENCHMARKSQL_HOME/plot/clients.plot
  mv clients.png $BENCHMARKSQL_HOME/${results_dir}/clients_${db_type}.png
  mv clients.txt $BENCHMARKSQL_HOME/${results_dir}/clients_${db_type}-1.txt
  #rm clients.txt
fi

echo "### Averages for test set $SET by scale and clients"
echo "<h1> Averages for test set $SET by scale and clients: </h1>">> $OUTFILE
echo "<table  border=\"1\">">>$OUTFILE
echo "<tr><td>数据规模</td><td>并发数 </td><td> 性能指标（tpmC）</td><td> 平均延时 </td><td> 延时90%< </td><td> 最大延时 </td></tr>">>$OUTFILE

echo "### Averages for test set $SET by scale and clients: ">> $OUTFILE_MD
echo >>$OUTFILE_MD
echo "数据规模 |并发数 | 性能指标（tpmC）| 平均延时 | 延时90%< | 最大延时 ">>$OUTFILE_MD
echo ":--------|:------|:----------------|:---------|:---------|:---------" >>$OUTFILE_MD

echo >3d.txt
for SCALE in $SCALES; do
  echo >scalingx-vlines.txt
	for c in $SETCLIENTS; do        
        if [ -f "$BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out" ] ; then
          echo "### Averages for test set $SET by scale $SCALE and client $c"
          tpmc=`grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 11 `
          if [ ! "$tpmc" = "" ] ; then            

              tpmc_total=`grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 11 |awk '{sum += $1};END {print sum}' `
              tpmc_cnt=`grep "Measured tpmC" $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 11 |wc -l `
              tpmc=`echo $tpmc_total $tpmc_cnt|awk '{print $1/$2}'`
              echo ${SCALE} ${c} $tpmc>>3d.txt

              TOTAL_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |awk '{sum += $1};END {print sum}'`
              MIN_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n|head -n 1`
              COUNT=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|wc -l`
              COUNT_90PERCENT=`echo $COUNT| awk '{printf("%d\n", $1 * 10 / 100);}'`
     
              LATENCY_AVG_IN_MS=`echo $TOTAL_IN_MS $COUNT| awk '{print $1 / $2 }'`
              LATENCY_90PERCENT_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n |tail -n $COUNT_90PERCENT |head -n 1`
              LATENCY_MAX_IN_MS=`grep "^STAT " $BENCHMARKSQL_HOME/${results_dir}/${db_type}_benchmark_${SCALE}_${c}_${TEST_TIME_IN_MIN}.out|cut -d " " -f 8  |sort -n|tail -n 1`

              echo "<tr><td>" ${SCALE} "</td><td>" ${c} "</td><td>" $tpmc "</td><td>" $LATENCY_AVG_IN_MS "</td><td>" $LATENCY_90PERCENT_IN_MS "</td><td>" $LATENCY_MAX_IN_MS "</td></tr>" >>$OUTFILE
              echo ${SCALE} "|" ${c} "|" $tpmc "|" $LATENCY_AVG_IN_MS "|" $LATENCY_90PERCENT_IN_MS "|" $LATENCY_MAX_IN_MS >>$OUTFILE_MD
              echo ${SCALE} " " ${c} " " $tpmc " " $LATENCY_AVG_IN_MS " " $LATENCY_90PERCENT_IN_MS " " $LATENCY_MAX_IN_MS >>scalingx-vlines.txt
          fi   
        fi    
    done
    echo >>scalingx-vlines.txt
    if [ ! "`cat scalingx-vlines.txt|wc -c`" = "1" ] ; then
      gnuplot $BENCHMARKSQL_HOME/plot/scalingx-vlines.plot
      mv scalingx-vlines.png $BENCHMARKSQL_HOME/${results_dir}/scaling-${SCALE}-vlines_${db_type}.png
      mv scalingx-vlines.txt $BENCHMARKSQL_HOME/${results_dir}/scaling-${SCALE}-vlines_${db_type}-1.txt
    fi  
    echo >>3d.txt
done    
echo "</table>" >>$OUTFILE
echo >>$OUTFILE_MD

if [ ! "`cat 3d.txt|wc -c`" = "1" ] ; then
  gnuplot $BENCHMARKSQL_HOME/plot/3d.plot
  mv 3d.png $BENCHMARKSQL_HOME/${results_dir}/3d_${db_type}.png
  mv 3d.txt $BENCHMARKSQL_HOME/${results_dir}/3d_${db_type}-1.txt
  #rm 3d.txt
fi  
