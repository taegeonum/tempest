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
5. --operator_type: dynamic_mts/naive/static_mts
  * dynamic_mts: Dynamic multi-timescale impl
  * static_mts: Static multi-timescale impl
  * naive: Naive impl
6. --timescales: timescales
  * format: (window_size,interval),(window_size2,interval2)...
  * unit: second
  * ex) (5,1)(10,2) -> two timescales: window size is 5seconds, interval is 1 second. window size is 10 seconds, interval is 2 seconds.
7. --bolts: the number of bolts
8. --input_interval: interval of sending input.
9. --caching_rate: caching rate for dynamic mts operator


# How to add timescales dynamically
* ./bin/mts_signal_sender.sh #parameters#

# Parameters
1. zookeeper address
2. mts identifier
3. window size
4. interval
5. type (addition/deletion)