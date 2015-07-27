
MTS_0=`cat $1/mts-rate0/initialTime`
MTS_02=`cat $1/mts-rate0_2/initialTime`
MTS_04=`cat $1/mts-rate0_4/initialTime`
MTS_06=`cat $1/mts-rate0_6/initialTime`
MTS_1000=`cat $1/mts-rate1000/initialTime`
NAIVE=`cat $1/naive/initialTime`
RG=`cat $1/rg/initialTime`
OTF=`cat $1/otf/initialTime`


if [ ${#MTS_0} -eq 0 ];then 
  MTS_0=1
fi

if [ ${#MTS_02} -eq 0 ];then 
  MTS_02=1
fi
if [ ${#MTS_04} -eq 0 ];then 
  MTS_04=1
fi
if [ ${#MTS_06} -eq 0 ];then 
  MTS_06=1
fi
if [ ${#MTS_1000} -eq 0 ];then 
  MTS_1000=1
fi
if [ ${#NAIVE} -eq 0 ];then 
  NAIVE=1
fi
if [ ${#RG} -eq 0 ];then 
  RG=1
fi
if [ ${#OTF} -eq 0 ];then 
  OTF=1
fi

echo $MTS_0
echo $MTS_02
echo $MTS_04
echo $MTS_06
echo $MTS_1000
echo $NAIVE
echo $RG
echo $OTF
gnuplot -e "folder='$1'; mts_rate0=$MTS_0; mts_rate0_2=$MTS_02; mts_rate0_4=$MTS_04; mts_rate0_6=$MTS_06; mts_rate1000=$MTS_1000; naive=$NAIVE; rg=$RG; otf=$OTF" plot.gnuplot 
