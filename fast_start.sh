#!/bin/bash

python fast_start.py -testname local_test -variable test -ts_path ts10 -data_path dataset/debs2016/largedata/post_output.dat -input_rate 100 -total_time 3600 -output_path log/tempest -operator_type FastSt -tradeoff_factor 100000.0

python fast_start.py -testname local_test -variable test -ts_path ts10 -data_path dataset/debs2016/largedata/post_output.dat -input_rate 100 -total_time 3600 -output_path log/tempest -operator_type TriOps -tradeoff_factor 100000.0

python fast_start.py -testname local_test -variable test -ts_path ts10 -data_path dataset/debs2016/largedata/post_output.dat -input_rate 100 -total_time 3600 -output_path log/tempest -operator_type Cuttyy -tradeoff_factor 100000.0
