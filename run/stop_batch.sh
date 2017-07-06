batch_pid=`ps -ef|grep batch.sh |grep -v grep |grep -v stop |tr -s " "|cut -d " " -f 2`
bench_pid=`ps -ef|grep runBenchmark.sh |grep -v grep |tr -s " "|cut -d " " -f 2`
echo "$batch_pid $bench_pid"
kill -9 $batch_pid 
kill -9 $bench_pid