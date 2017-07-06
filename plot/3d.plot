set term png
set size 0.75,0.75
set output "3d.png"
set title "benchmarksql tpmC"
set xlabel "Scaling factor"
set xrange [0:*] 
set ylabel "Clients"
set yrange [0:*] 
set zlabel "tpmC"
set zrange [0:*]
#set dgrid3d 30,30
#set hidden3d
#set pm3d
#splot "3d.txt" u 1:2:3 with lines

set ticslevel 0
zmin = 0
zr(x) = (x==0 ? zmin : x)
splot '3d.txt' using 1:2:(zr($3)) with impulses palette
