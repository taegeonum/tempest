Tempest project

# Pre-requisite

0. Install and start zookeeper
  - download from https://zookeeper.apache.org/
  - tar
  - cp conf/zoo_sample.cfg conf/zoo.cfg
  - ./bin/zkServer.sh start

# How to run WordCount example

1. mvn clean package 
2. ./bin/run_test.sh #parameter_file#
  - ex) ./bin/run_test.sh test_param

# Parameters
1. --log_dir: logging directory
2. --test_name: test name
3. --spouts: the number of spouts 
4. --total_time : total elapsed time to test
5. --mts_operator: mts/naive/otf/rg
  * dynamic_mts: Dynamic multi-timescale impl
  * static_mts: Static multi-timescale impl
  * naive: Naive impl
  * otf: On-the-fly sharing (input sharing) impl
6. --timescales: timescales
  * format: (window_size,interval),(window_size2,interval2)...
  * unit: second
  * ex) (5,1)(10,2) -> two timescales: window size is 5seconds, interval is 1 second. window size is 10 seconds, interval is 2 seconds.
7. --bolts: the number of bolts

