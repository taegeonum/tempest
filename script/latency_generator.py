import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('directory', type=str)
parser.add_argument('percentile', type=int)

args = parser.parse_args()
pattern = re.compile("[0-9]+-[0-9]+-latency")
trim = 100

latencies = []

def parse_file(file_path):
  interval = long(file_path.split("/")[-1].split("-")[1])

  f = open(file_path, 'r')
  lines = f.readlines();

  i = 1
  for line in lines:
    elapsed_time = i * interval
    i += 1
    if elapsed_time >= trim:
      latency = long(line.split("\t")[0])
      latencies.append(latency)
    
  f.close()

def parse_directory(directory, percentile):
  onlyfiles = [ f for f in listdir(directory) if isfile(join(directory,f)) and pattern.match(f)]
  for f in onlyfiles:
    parse_file(join(directory,f))

  # calculate 95 or 99 % latencies
  # sort
  latencies.sort()
  last = int(percentile * 0.01 * len(latencies))
  percentile_latency = latencies[last]
  print str(percentile),"% latency: ", percentile_latency

parse_directory(args.directory, args.percentile)
