"""
This scripts calculates average latency and counts for MTSS logs in designated location
The results should be in separate directories per runnings.
"""

import os, sys, argparse
import numpy as np

parser = argparse.ArgumentParser(description='Calculates average latency and counts for MTSS logs,')
parser.add_argument('path', metavar='PATH', type=str, help='Path to the MTSS result folder')

args = parser.parse_args()

avg_latency = {}
avg_throughput = {}

path = args.path

for dirname in os.listdir(path):
    new_path = os.path.join(path, dirname)
    if not os.path.isdir(new_path):
        continue  

    for filename in os.listdir(new_path):
        print filename
        category = filename.split("-")[0]
        if category not in avg_latency:
            avg_latency[category] = {}
            avg_throughput[category] = {}

        window_size = filename.split("-")[2]
        if window_size not in avg_latency[category]:
            avg_latency[category][window_size] = []
            avg_throughput[category][window_size] = []

        data_file = open(os.path.join(new_path, filename))
        lines = []
        latencies = []
        throughputs = []
        lines = [line.strip() for line in data_file]
    
        for i in range(len(lines)/2, len(lines), 1):
            if(len(lines[i].split()) < 2):
                continue
            latencies += [int(lines[i].split()[0])]
            throughputs += [int(lines[i].split()[1])]

        avg_latency[category][window_size] += [np.average(latencies)]
        avg_throughput[category][window_size] += [np.average(throughputs)]

for category in avg_latency.keys():
    print "%s Implementation" % category
    for window_size in avg_latency[category].keys():
        print "WindowSize = %s, latency = %d, throughput = %d" % (window_size, np.average(avg_latency[category][window_size]), np.average(avg_throughput[category][window_size]))
    print "\n"
