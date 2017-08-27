
import sys

fname = sys.argv[1]
output_name = sys.argv[2]
f = open(fname)
of = open(output_name, "w")


for line in f:
  splitted = line.split("|")
  uid = splitted[2]
  of.write(uid)
  of.write("\n")

f.close()
of.close()
