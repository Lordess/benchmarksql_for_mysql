set term png
set size 0.75,0.75
set output "scaling.png"
myfile = "scaling.txt"

set key right bottom title " "
set title "benchmarksql tpmC with Latency"
set grid xtics ytics
set xlabel "Scaling factor"
set ylabel "tpmC"
set yrange [0:*] 
set y2label "Avg Latency/ 90% Latency <"
set y2range [0:*] 
# y2tics sets the increment between ticks, not their number
set y2tics autofreq
#将离散的点拟合曲线，便于观察趋势
y1(x) = b1*x + c1
fit y1(x) myfile using 1:3 via b1,c1
y2(x) = b2*x + c2
fit y2(x) myfile using 1:4 via b2,c2
y3(x) = b3*x + c3
fit y3(x) myfile using 1:5 via b3,c3
plot \
  myfile using 1:3 axis x1y1 title 'TPS' with lines,\
  myfile using 1:4 axis x2y2 title 'Avg Latency', \
  myfile using 1:5 axis x2y2 title '90% Latency <', \
  y1(x) axis x1y1 notitle, y2(x) axis x2y2 notitle, y3(x) axis x2y2 notitle


