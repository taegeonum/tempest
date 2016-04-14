import os, sys, argparse
from os import listdir
from os.path import isfile, join
import re


parser = argparse.ArgumentParser(description='num thread, timescale and computation reuse')
#parser.add_argument('initial_time',  type=long, help='initial_time')
parser.add_argument('directory', type=str)

args = parser.parse_args()
pattern = re.compile("^60-1$")

def parse_file(file_path, initial_time, start, end):
  interval = long(file_path.split("/")[-1].split("-")[1]) * 1000

  f = open(file_path, 'r')
  fw = open(file_path + "-throughput", 'w')
  lines = f.readlines();

  i = 1
  for line in lines:
    actual_time = long(line.split("\t")[0])
    elapsed = actual_time - initial_time 
    count = long(line.split("\t")[1])
    if elapsed >= start and elapsed < end:
      thp = count / (60)
      print "elapsed: ", elapsed, ", thp: ", thp
      fw.write(str(elapsed)+ "\t"+ str(thp) + "\n")
    i += 1
    
  f.close()
  fw.close()

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
    parse_file(join(directory,f), initial_time, start, end)
parse_directory(args.directory)
