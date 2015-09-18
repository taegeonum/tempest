import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re

parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
parser.add_argument('directory', type=str)

args = parser.parse_args()
trim = 100

def calculate_avg(arr):
  vals = map(lambda x: float(x.split("\t")[1].rstrip()), arr)
  return reduce(lambda x,y: x+y, vals)/ len(vals)


def parse_file(directory):
  # cpu
  cpu = open(join(directory,"cpu"), 'r')
  cpu_lines = cpu.readlines()[trim:-1];

  mem = open(join(directory,"memory"), 'r')
  mem_lines = mem.readlines()[trim:-1];
  print "avg cpu: ", calculate_avg(cpu_lines), " avg_mem: ", calculate_avg(mem_lines)

parse_file(args.directory)
