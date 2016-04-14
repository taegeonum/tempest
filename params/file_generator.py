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

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('num_threads',  type=int, help='num threads')
parser.add_argument('timescales', type=int, help='num timescales')
parser.add_argument('caching_rate', type=float, help='caching rate')

#args = parser.parse_args()

def create_file(operator_type, num_threads, num_timescales, caching_rate):
  directory = "0914-evaluation-" + operator_type
  file_name = str(num_threads) + "-" + str(num_timescales) + "-" + str(caching_rate)
  if not os.path.exists(directory):
        os.makedirs(directory)

  f = open(directory + "/" + file_name, "w")
  f.write("--log_dir=./logs/" + directory + "/\n")
  f.write("--test_name="+file_name + "\n")
  f.write("--spouts=20\n")
  f.write("--bolts=1\n")
  f.write("--caching_rate=" + str(caching_rate) + "\n")
  f.write("--total_time=1200\n")
  f.write("--input=random\n")
  f.write("--operator_type=" + operator_type + "\n")
  f.write("--num_threads=" + str(num_threads) + "\n")
  f.write("--input_interval=1\n")
  f.write("--timescales=" + get_timescales(num_timescales) + "\n")
  f.close()

def get_timescales(num_timescales):
  if num_timescales == 2:
    return "(300,2)(1200,10)"
  elif num_timescales == 4:
    return "(300,2)(600,5)(900,10)(1200,10)"
  elif num_timescales == 6:
    return "(300,2)(450,2)(600,5)(900,10)(1050,5)(1200,10)"
  elif num_timescales == 8:
    return "(300,2)(400,4)(450,2)(600,5)(750,5)(900,10)(1050,5)(1200,10)"
  elif num_timescales == 10:
    return "(60,1)(120,2)(180,2)(240,2)(300,5)(360,5)(420,5)(480,5)(540,5)(600,10)"
  elif num_timescales == 12:
    return "(300,2)(400,4)(500,5)(600,5)(660,10)(750,5)(800,10)(900,10)(1000,10)(1050,5)(1100,10)(1200,10)"
  elif num_timescales == 16:
    return "(300,2)(350,2)(380,3)(400,4)(450,2)(500,5)(600,5)(660,10)(750,5)(800,10)(900,10)(1000,10)(1050,5)(1100,10)(1150,20)(1200,10)"
  elif num_timescales == 32:
    return "(300,2)(310,5)(320,4)(350,2)(360,5)(380,3)(400,4)(420,10)(430,5)(450,2)(460,5)(500,5)(550,10)(580,10)(600,5)(640,5)(660,10)(700,10)(720,5)(750,5)(800,10)(850,10)(870,5)(900,10)(950,10)(1000,10)(1050,5)(1070,30)(1100,10)(1120,20)(1150,20)(1200,10)"


#create_file(args.num_threads, args.timescales, args.caching_rate)
for thread in [16]:
  for timescale in [10]:
    for caching_rate in [0]:
      for operator_type in ["dynamic_random"]:
        create_file(operator_type, thread, timescale, caching_rate)
