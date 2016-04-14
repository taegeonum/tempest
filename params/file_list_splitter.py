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
from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser(description='')
parser.add_argument('num_node',  type=int, help='num node')
parser.add_argument('index',  type=int, help='index')

args = parser.parse_args()

mypath = "./0818-additional-timescales-tree-agg-true"
files = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]

#print files

index = args.index
num_my_files = len(files) / args.num_node

my_files = files[(num_my_files * index):(num_my_files*(index+1))]
my_files.sort()
#print my_files


f = open("../start.sh", "w")
f.write(reduce(lambda x,y: x+y, map(lambda x: "./bin/run_test.sh params/0818-additional-timescales-tree-agg-true/" + x+"\n", my_files)))
f.write(reduce(lambda x,y: x+y, map(lambda x: "./bin/run_test.sh params/0818-additional-timescales-tree-agg-false/" + x+"\n", my_files)))
f.close()

os.chmod("../start.sh", 0755)
