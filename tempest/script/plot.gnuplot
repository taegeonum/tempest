set size 1,.7
#set terminal postscript eps enhanced color
#set output 'wordcount.eps'
set origin 0.0,0.08

set xlabel "Elapsed time (sec)"
set ylabel font "Times-Roman, 22"
set xlabel font "Times-Roman, 22" offset -3
#set key font "Times-Roman, 22"
#set key bottom left spacing 2
#set key width 6 height 2

#set format y "%.1t{/Symbol \327}10^{%L}"
#set xr [1:5]
#set pointsize 2
#set xtics 1

#set xtics font "Times-Roman, 22" 
#set ytics 80000000 font "Times-Roman, 20"

#mts_rate0 = 1427019570131
#mts_rate0_2=1427020206138
#mts_rate0_4=1427020842136
#mts_rate0_6=1427021478129
#naive=1427018934048
#rg=1427022114128
#otf=1427022752134

adjust1(x,y) = (x - y)/1000
noe(x) = x/5

set term aqua 1
set key outside top

set xtics 60
set ylabel "cpu load" offset -3
set yr [0:1]
plot folder."/mts-rate0/cpu" using (adjust1($1, mts_rate0)):2 w lp ls 1 lw 2 title "mts:p=0", \
  folder."/naive/cpu" using (adjust1($1, naive)):2 w lp ls 2 lw 2 title "naive", \
  folder."/rg/cpu" using (adjust1($1, rg)):2 w lp ls 3 lw 2 title "rg", \
  folder."/otf/cpu" using (adjust1($1, otf)):2 w lp ls 4 lw 2 title "otf", \
  folder."/mts-rate0_2/cpu" using (adjust1($1, mts_rate0_2)):2 w lp ls 5 lw 2 title "mts:p=0.2", \
  folder."/mts-rate0_4/cpu" using (adjust1($1, mts_rate0_4)):2 w lp ls 6 lw 2 title "mts:p=0.4", \
  folder."/mts-rate0_6/cpu" using (adjust1($1, mts_rate0_6)):2 w lp ls 7 lw 2 title "mts:p=0.6", \
  folder."/mts-rate1000/cpu" using (adjust1($1, mts_rate1000)):2 w lp ls 8 lw 2 title "mts:p=1000"

unset key
set term aqua 3
set autoscale y
set ylabel "memory usage(byte)" offset -3
plot folder."/mts-rate0/memory" using (adjust1($1, mts_rate0)):2 w lp ls 1 lw 2 title "mts:p=0", \
  folder."/naive/memory" using (adjust1($1, naive)):2 w lp ls 2 lw 2 title "naive", \
  folder."/rg/memory" using (adjust1($1, rg)):2 w lp ls 3 lw 2 title "rg", \
  folder."/otf/memory" using (adjust1($1, otf)):2 w lp ls 4 lw 2 title "otf", \
  folder."/mts-rate0_2/memory" using (adjust1($1, mts_rate0_2)):2 w lp ls 5 lw 2 title "mts:p=0.2", \
  folder."/mts-rate0_4/memory" using (adjust1($1, mts_rate0_4)):2 w lp ls 6 lw 2 title "mts:p=0.4", \
  folder."/mts-rate0_6/memory" using (adjust1($1, mts_rate0_6)):2 w lp ls 7 lw 2 title "mts:p=0.6", \
  folder."/mts-rate1000/memory" using (adjust1($1, mts_rate1000)):2 w lp ls 8 lw 2 title "mts:p=1000"


set term aqua 4
set autoscale y
set ylabel "num of execution per sec" offset -3
plot folder."/mts-rate0/slicedWindowExecution" using (adjust1($1, mts_rate0)):2 w lp ls 1 lw 2 title "mts:p=0", \
  folder."/naive/slicedWindowExecution" using (adjust1($1, naive)):2 w lp ls 2 lw 2 title "naive", \
  folder."/rg/slicedWindowExecution" using (adjust1($1, rg)):2 w lp ls 3 lw 2 title "rg", \
  folder."/otf/slicedWindowExecution" using (adjust1($1, otf)):2 w lp ls 4 lw 2 title "otf", \
  folder."/mts-rate0_2/slicedWindowExecution" using (adjust1($1, mts_rate0_2)):2 w lp ls 5 lw 2 title "mts:p=0.2", \
  folder."/mts-rate0_4/slicedWindowExecution" using (adjust1($1, mts_rate0_4)):2 w lp ls 6 lw 2 title "mts:p=0.4", \
  folder."/mts-rate0_6/slicedWindowExecution" using (adjust1($1, mts_rate0_6)):2 w lp ls 7 lw 2 title "mts:p=0.6", \
  folder."/mts-rate1000/slicedWindowExecution" using (adjust1($1, mts_rate1000)):2 w lp ls 8 lw 2 title "mts:p=1000"

