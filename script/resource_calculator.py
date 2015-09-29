import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('directory', type=str)

args = parser.parse_args()

def sum_arr(arr):
  vals = map(lambda x: float(x.split("\t")[1].rstrip()), arr)
  vals = filter(lambda x: x > 0, vals)
  return reduce(lambda x,y: x+y, vals)

def calculate_avg(arr):
  return sum_arr(arr)/ len(arr)

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


def parse_file(directory):
  initial_time = get_initial_time(directory)
  end = get_total_time(directory)
  start = end/2

  # cpu
  cpu = open(join(directory,"cpu"), 'r')
  cpu_lines = cpu.readlines()[start:end];

  mem = open(join(directory,"memory"), 'r')
  mem_lines = mem.readlines()[start:end];

  # thp
  thp = open(join(directory,"slicedWindowExecution"), 'r')
  thp_lines = thp.readlines()[start:end]

  avg_thp = sum_arr(thp_lines)/((end - start))

  # latency
  #ltc = open(join(directory, "total_latencies"), 'r')
  #ltc_lines = ltc.readlines()

  print "avg cpu\t", calculate_avg(cpu_lines),"\n avg_mem: ", calculate_avg(mem_lines), "\n avg_thp: ", avg_thp#, "\n avg_ltc: ", calculate_avg(ltc_lines)

parse_file(args.directory)


### latency
pattern = re.compile("^[0-9]+-[0-9]+$")

def parse_output_file(file_path, initial_time, start, end):
  latencies = []
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

def calculate_ltc_avg(arr):
  vals = map(lambda x: x[1], arr)
  return reduce(lambda x,y: x+y, vals)/ len(vals)

def parse_directory(directory):
  total_latencies = []
  initial_time = get_initial_time(directory)
  end = get_total_time(directory) * 1000
  start = end/2
  onlyfiles = [ f for f in listdir(directory) if isfile(join(directory,f)) and pattern.match(f)]
  onlyfiles = sorted(onlyfiles, key=lambda x: int(x.split("-")[0]))
  for f in onlyfiles:
    latencies = parse_output_file(join(directory,f), initial_time, start, end)
    total_latencies = total_latencies + latencies
    print f + "\t" + str(calculate_ltc_avg(latencies))

  sorted_latencies = sorted(latencies, key=lambda x: x[0])
  fw = open(join(directory,"total_latencies"), "w")
  print "total_avg_latency: ", calculate_ltc_avg(total_latencies)
  fw.write("\n".join(map(lambda x: "\t".join(map(lambda y: str(y), x)), sorted_latencies)))

parse_directory(args.directory)
