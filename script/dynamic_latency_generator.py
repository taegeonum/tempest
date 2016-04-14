import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('directory', type=str)

args = parser.parse_args()
pattern = re.compile("^[0-9]+-[0-9]+$")

def parse_output_file(file_path, start, end):
  latencies = []
  interval = long(file_path.split("/")[-1].split("-")[1]) * 1000

  f = open(file_path, 'r')
  lines = f.readlines();

  i = 1
  initial_time = 0
  for line in lines:
    if i == 1:
      initial_time = long(line.split("\t")[0])
    elapsed = i * interval
    expected_time = initial_time + i * interval
    if elapsed >= start and elapsed < end:
      actual_time = long(line.split("\t")[0])
      latency = actual_time - expected_time
      #print "actual_time: ", actual_time, ", expected_time: ", expected_time
      if latency < 0:
        latency = 0
      latencies.append([actual_time-initial_time-start, latency])
    i += 1
  f.close()
  return latencies

def get_initial_time(directory):
  f = open(join(directory,"initialTime"), "r")
  return long(f.readline())

def get_total_time(directory):
  f = open(join(directory,"conf"), "r")
  lines = f.readlines()
  total_time = 0
  for line in lines:
    splited = line.split(": ")
    if splited[0] == "TOTAL_TIME":
      return long(splited[1])
  raise Exception()

def calculate_avg(arr):
  vals = map(lambda x: x[1], arr)
  return reduce(lambda x,y: x+y, vals)/ len(vals)

def parse_directory(directory):
  total_latencies = []
  start = 150 * 1000
  end = get_total_time(directory) * 1000
  onlyfiles = [ f for f in listdir(directory) if isfile(join(directory,f)) and pattern.match(f)]
  onlyfiles = sorted(onlyfiles, key=lambda x: int(x.split("-")[0]))
  for f in onlyfiles:
    latencies = parse_output_file(join(directory,f), start, end)
    total_latencies = total_latencies + latencies
    print f + "\t" + str(calculate_avg(latencies))

  sorted_latencies = sorted(latencies, key=lambda x: x[0])
  fw = open(join(directory,"total_latencies"), "w")
  print "total_avg_latency: ", calculate_avg(total_latencies)
  fw.write("\n".join(map(lambda x: "\t".join(map(lambda y: str(y), x)), sorted_latencies)))

parse_directory(args.directory)
