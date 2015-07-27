if [ "$#" -ne 2 ]; then
 echo "Illegal number of parameters"
fi

num=$1
path=$2
str_mem="";
str_bytes="";
for (( i=0; i<=$num; i++ ))
do
 echo $i
 str_mem=$str_mem"$path-$i/eomtaegeon-ui-MacBook-Pro-2.local/wcbolt/memory "
 str_bytes=$str_bytes"$path-$i/eomtaegeon-ui-MacBook-Pro-2.local/wcbolt/bytes "
done

echo $path
echo "python aggregate.py $path/agg_memory $str_mem"
echo "python aggregate.py $path/agg_bytes $str_bytes"

python aggregate.py $path/agg_memory $str_mem
python aggregate.py $path/agg_bytes $str_bytes

