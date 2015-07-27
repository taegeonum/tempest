"""
This scripts calculates average latency and counts for MTSS logs in designated location
The results should be in separate directories per runnings.
"""

import os, sys, argparse
import numpy as np

parser = argparse.ArgumentParser(description='Aggregate values')
parser.add_argument('dest',  type=str, help='dest')
parser.add_argument('srcs', nargs='+', type=str, help='Path to the MTSS result folder')

args = parser.parse_args()

print args
dest = args.dest
srcs = args.srcs

def find(arr, elem):
  for i in range(0, len(arr) - 1):
    if (arr[i][0] < elem[0] and arr[i+1][0] >= elem[0]):
      return i
  return len(arr) - 1



def open_file(path):
  file1 = open(path)
  list1 = map(lambda x: map(lambda y: long(y), x.rstrip('\n').split("\t")), file1.readlines())
  return list1



def aggregate(list1, list2): 
  min_len = len(list1) if len(list1) < len(list2) else len(list2)

  #print "len_list1: ", len(list1), " len_list2: ", len(list2), " min_len: ", min_len
  #print list1
  #print list2

  #print "start"
  for i in range(0, min_len):
    index = find(list1, list2[i])
    #print "i: ", i , " index: ", index
    new_elem = []
    new_elem.append(list1[index][0])
    new_elem.append(list1[index][1] + list2[i][1])
    list1[index] = new_elem

  return list1

def timestampToElapsedTime(initial_time, x):
  x[0] = x[0] - initial_time
  return x

def elemToStr(e):
  return map(lambda x: str(x), e)

agg_list = reduce(aggregate, map(open_file, srcs))
initial_time = agg_list[0][0]

aggregated_file_str = "\n".join(map(lambda x: "\t".join(elemToStr(x)), agg_list))
print aggregated_file_str

new_file = open(dest, "w")
new_file.write(aggregated_file_str)
new_file.close()

