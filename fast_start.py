import math
import os
import subprocess
import time
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-target_class", type=str, default="TwitterEvaluation")
#parser.add_argument("param", type=str)
parser.add_argument("-operator_type", type=str)
parser.add_argument("-reuse", type=float, default=1.0)
parser.add_argument("-testname", type=str)
parser.add_argument("-variable", type=str)
parser.add_argument("-ts_path", type=str)
parser.add_argument("-data_path", type=str)
parser.add_argument("-input_rate", type=int)
parser.add_argument("-total_time", type=int)
parser.add_argument("-num_threads", type=int, default=4)
parser.add_argument("-output_path", type=str, default="log/fast-icde/")


args = parser.parse_args()

classpath = "target/tempest-0.11-SNAPSHOT.jar"
target_class = "vldb.evaluation." + args.target_class

timescales = subprocess.check_output("cat %s" % args.ts_path, shell=True)
timescales= timescales.strip('\n')


cmd = "java -cp %s %s --test_name=%s --variable=%s --timescales=\"%s\" --data_path=%s --input_rate=%d --total_time=%d --num_threads=%d --output_path=%s --operator_type=%s --reuse=%f" % (classpath, target_class, args.testname, args.variable, timescales, args.data_path, args.input_rate, args.total_time, args.num_threads, args.output_path, args.operator_type, args.reuse)


print("==== cmd ====")
print(cmd)
subprocess.call(cmd, shell=True)
