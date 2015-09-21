import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('directory', type=str)

args = parser.parse_args()

def calculate_avg(arr):
  vals = map(lambda x: float(x.split("\t")[1].rstrip()), arr)
  return reduce(lambda x,y: x+y, vals)/ len(vals)

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
  start = 3000
  end = get_total_time(directory)

  # cpu
  cpu = open(join(directory,"cpu"), 'r')
  cpu_lines = cpu.readlines()[start:end];

  mem = open(join(directory,"memory"), 'r')
  mem_lines = mem.readlines()[start:end];

  # thp
  thp = open(join(directory,"3600-60"), 'r')
  thp_lines = thp.readlines()[start/60:end]

  # latency
  ltc = open(join(directory, "total_latencies"), 'r')
  ltc_lines = ltc.readlines()

  print "avg cpu\t", calculate_avg(cpu_lines),"\n avg_mem: ", calculate_avg(mem_lines), "\n avg_thp: ", calculate_avg(thp_lines)/3600, "\n avg_ltc: ", calculate_avg(ltc_lines)

parse_file(args.directory)
