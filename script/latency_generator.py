import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('directory', type=str)

args = parser.parse_args()
pattern = re.compile("[0-9]+-[0-9]+-latency")

latencies = []

def parse_file(file_path):
  interval = long(file_path.split("/")[-1].split("-")[1])

  f = open(file_path, 'r')
  lines = f.readlines();

  i = 1
  for line in lines:
    i += 1
    latency = long(line.split("\t")[0])
    latencies.append(latency)
    
  f.close()

def parse_directory(directory):
  onlyfiles = [ f for f in listdir(directory) if isfile(join(directory,f)) and pattern.match(f)]
  for f in onlyfiles:
    parse_file(join(directory,f))

  # calculate 95 or 99 % latencies
  # sort
  latencies.sort()
  latency_95 = int(0.95 * len(latencies))
  latency_99 = int(0.99 * len(latencies))
  print "95% latency: ", latencies[latency_95], "\n", "99% latency: ", latencies[latency_99]

parse_directory(args.directory)
