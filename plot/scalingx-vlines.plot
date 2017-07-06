file="scalingx-vlines.txt"
set term png
#set size 0.75,0.75
set output "scalingx-vlines.png"
set grid x y2
set key right bottom title " "

set title "benchmarksql tpmC with Latency"
set xlabel "Concurrent Users"
set ylabel "tpmC"
set y2label "Avg Latency/ 90% Latency <"
set ytics nomirror
set y2tics
set tics out
set yrange [0:*] 
set autoscale  y
set autoscale y2

# 先找出并发数为1时的基准吞吐量值
#base0 = real( system("grep scalingx-vlines.txt|head -n 1|tr -s ' '|cut -d ' ' -f 2") )
#c1 = real( system("grep scalingx-vlines.txt|head -n 1|tr -s ' '|cut -d ' ' -f 3") )
base0 = real( "`grep ' ' scalingx-vlines.txt|head -n 1|tr -s ' '|cut -d ' ' -f 2`" )
c1 = real("`grep ' ' scalingx-vlines.txt|head -n 1|tr -s ' '|cut -d ' ' -f 3`")
# 将统一扩展性公式转换为二次多项式公式，以便正确计算出公式系数。
y0(x) = a0 * x**2 + b0 * x

# 下面的公式是gnuplot的拟合功能：$2、$3指第二、三列，
# ($2 - base0)是第二列的值与base0变量的差作为x
# ( $2 / ($3 / c1) - 1.0 )是第三列的值经过运算后，作为y。有了x和y之后，就能计算出a0和b0
fit y0(x) file using ($2 - base0):( $2 / ($3 / c1) - 1.0 ):($3) via a0, b0
# 算出a1、b1、c1之后，根据统一扩展性公式，绘制其趋势曲线
b1 = a0
a1 = b0 - a0
y1_title=sprintf("USL(a1=%f,b1=%f,c1=%f)",a1, b1, c1)
y1(x) = x * c1 / (1 + a1* (x - 1) + b1 * x  * (x - 1) )

#将离散的点拟合曲线，便于观察趋势
y2(x) = a2*x**2 + b2*x + c2
fit y2(x) file using 2:4 via a2,b2,c2
y3(x) = a3*x**2 + b3*x + c3
fit y3(x) file using 2:5 via a3,b3,c3
plot \
   file using 2:3 axis x1y1 title 'tpmC' with lines, \
   y1(x) axis x1y1 title y1_title, \
   file using 2:4 axis x2y2 title 'Avg Latency', \
   file using 2:5 axis x2y2 title '90% Latency <', \
   y2(x) axis x2y2 notitle, y3(x) axis x2y2 notitle
