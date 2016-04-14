* Applications
 - Word count
 - TopK
 - LinReg
 - Avg

* How to run applications?

1. Required Parameters
 - test_name: the name of this test. This is prefix of log folder
 - log_dir: the path of logging directory. All of the outputs is written to this folder.
 - timescales: static timescales. ex) (30,2)(60,5)(90,6)
 - total_time: the total running time of this application (time unit is sec)
 - operator_type: which multi-time operator do you want to run?
  - dynamic_st: dynamic SpanTracker operator
  - static_st: static SpanTracker operator
  - naive: naive operator

2. Optional Parameters
 - num_keys: the number of input keys (Currently, we evaluate zipf distrib random dataset)
 - input_rate: inputs/sec
 - add_timescales: Additional timescales for dynamic mts operator.
  - ex) (40,2)(50,3)(60,4)
  - The above three timescales will be added dynamically at runtime.
 - ts_add_interval: The interval of addition of timescale (sec).
  - If ts_add_interval=3, then it adds the additional timescales every 3 seconds.

3. Run applications
 - static Word count (static multi-time operator test)
  - run `bin/run_test.sh params`
 - dynamic Word count (dynamic mts operator test)
  - run `bin/run_dynamic_test.sh params`
 - static top-k
  - run `bin/run_topk.sh params`
 - dynamic top-k
  - run `bin/run_dynamic_topk.sh params`
 - static avg
  - run `bin/run_avg.sh params`
 - dynamic avg
  - run `bin/run_dynamic_avg.sh params`
 - static linreg
  - run `bin/run_linreg.sh params`