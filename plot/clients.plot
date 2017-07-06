set term png
set size 0.75,0.75
set output "clients.png"
set title "benchmarksql tpmC"
set grid xtics ytics
set xlabel "Clients"
set ylabel "tpmC"
plot \
  "clients.txt" using 1:2 axis x1y1 title 'tpmC' with lines


