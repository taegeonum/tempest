"""
--log_dir=./logs/
--test_name=2-16-0
--spouts=70
--bolts=1
--caching_rate=0.0
--total_time=1800
--input=random
--operator_type=dynamic_mts
--num_threads=2
--input_interval=1
--timescales=(300,2)(350,2)(380,3)(400,4)(450,2)(500,5)(600,5)(660,10)(750,5)(800,10)(900,10)(1000,10)(1050,5)(1100,10)(1150,20)(1200,10)
"""                                                                                                                                        
import os, sys, argparse

#parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
#parser.add_argument('num_threads',  type=int, help='num threads')
#parser.add_argument('timescales', type=int, help='num timescales')
#parser.add_argument('caching_rate', type=float, help='caching rate')

#args = parser.parse_args()

uniform2={"ts": "(100,1)(200,2)", "num_timescale": "2", "timescale_type": "uniform", "total_time": "450"}
uniform5={"ts": "(100,1)(200,2)(300,5)(400,10)(500,1)", "num_timescale": "5", "timescale_type": "uniform", "total_time": "1100"}
uniform10={"ts": "(100,1)(200,2)(300,5)(400,10)(500,1)(600,2)(700,5)(800,10)(900,1)(1000,2)", "num_timescale": "10", "timescale_type": "uniform", "total_time": "2100"}
lskewed={"ts": "(10,1)(20,2)(30,5)(60,10)(130,1)(220,2)(340,5)(510,10)(730,1)(1000,2)", "num_timescale": "10", "timescale_type": "lskewed", "total_time": "2100"}
rskewed={"ts": "(50,1)(150,2)(670,5)(740,10)(800,1)(840,2)(890,5)(930,10)(965,1)(1000,2)", "num_timescale": "10", "timescale_type": "rskewed", "total_time": "2100"}

param_dir="params/1002-test/"
log_dir="./logs/1002-test/"
spouts="1"
caching_prob="0"
total_times=["2500"]
input_type="zipfian"
input_path="/"
operator_types=["naive"]
num_threads=["32"]
input_interval="1"
input_rates=["100000"]
zipfian_constant="0.99"
num_keys=["100000"]
timescale_type="uniform"
applications=["topk"]
setting=uniform2

def create_file(total_time, operator_type, num_thread, input_rate, num_key, timescale,num_timescale, application):
  directory = param_dir
  file_name = application + "-" + operator_type + "-" + num_thread + "-" + input_rate + "-" + num_key + "-" + num_timescale + "-" + setting["timescale_type"]
  if not os.path.exists(directory):
        os.makedirs(directory)

  f = open(directory + "/" + file_name, "w")
  f.write("--log_dir=" + log_dir + "\n")
  f.write("--test_name="+file_name + "\n")
  f.write("--spouts=" + spouts + "\n")
  f.write("--num_split_bolt=15\n")
  f.write("--bolts=1\n")
  f.write("--caching_prob=" + caching_prob + "\n")
  f.write("--total_time=" + total_time + "\n")
  f.write("--input=" + input_type + "\n")
  f.write("--input_path=/\n")
  f.write("--operator_type=" + operator_type + "\n")
  f.write("--num_threads=" + num_thread + "\n")
  f.write("--input_interval=" + input_interval + "\n")
  f.write("--input_rate=" + input_rate + "\n")
  f.write("--zipfian_constant=" + zipfian_constant + "\n")
  f.write("--num_keys=" + num_key + "\n")
  f.write("--timescales=" + timescale + "\n")
  f.close()


#create_file(args.num_threads, args.timescales, args.caching_rate)
for total_time in total_times:
  for operator_type in operator_types:
    for num_thread in num_threads:
      for input_rate in input_rates:
        for num_key in num_keys:
          for application in applications:
            total_time = setting["total_time"]
            timescale = setting["ts"]
            num_timescale = setting["num_timescale"]
            create_file(total_time, operator_type, num_thread, input_rate, num_key, timescale,num_timescale, application)

