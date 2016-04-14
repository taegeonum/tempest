import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('directory', type=str)

args = parser.parse_args()
pattern = re.compile("^[0-9]+-[0-9]+$")
latencies = [] 

def parse_output_file(file_path, initial_time, start, end):
  interval = long(file_path.split("/")[-1].split("-")[1]) * 1000

  f = open(file_path, 'r')
  lines = f.readlines();

  i = 1
  for line in lines:
    elapsed = i * interval
    expected_time = initial_time + i * interval
    if elapsed >= start and elapsed < end:
      actual_time = long(line.split("\t")[0])
      latency = actual_time - expected_time
      print "actual_time: ", actual_time, ", expected_time: ", expected_time
      if latency < 0:
        latency = 0
      latencies.append([actual_time-initial_time-start, latency])
    i += 1
  f.close()

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


def parse_directory(directory):
  initial_time = get_initial_time(directory)
  start = 100 * 1000
  end = get_total_time(directory) * 1000
  onlyfiles = [ f for f in listdir(directory) if isfile(join(directory,f)) and pattern.match(f)]
  for f in onlyfiles:
    parse_output_file(join(directory,f), initial_time, start, end)

  sorted_latencies = sorted(latencies, key=lambda x: x[0])
  fw = open(join(directory,"total_latencies"), "w")
  fw.write("\n".join(map(lambda x: "\t".join(map(lambda y: str(y), x)), sorted_latencies)))

parse_directory(args.directory)
