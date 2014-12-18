
MTSS project

#How to run 

1. Download storm and install 
http://mirror.apache-kr.org/storm/apache-storm-0.9.3/apache-storm-0.9.3.tar.gz

2. Add $STORM_DIR$/bin to $PATH$ 

3. Download MTSS code 
https://github.com/swsnu/MTSS/tree/refactoring-mtss

4. Configure parameter

5. start bin/run.sh with params 
Ex) bin/run.sh params. Example parameter file is in bin/params 


# Parameter

1. local : true / false
  - Is this local mode or cluster mode. default is true
  - To run in cluster mode, you need to setup the Storm cluster. 

2. app_name: MTSSTopology / NaiveTopology / WordGroupingMTSSTopology / WordGroupingNaiveTopology 
  - MTSSTopology: Top-k wordcount in mtss topology
  - NaiveTopology: Top-k wordcount in naive topology
  - WordGroupingMTSSTopology: Initial word character histogram in mtss topology
  - WordGroupingNaiveTopology: Initial word character histogram in naive topology

3. num_workers: the number of workers

4. input_interval: input interval ( ms )

5. num_spout: the number of spout

6. num_bolt: the number of bolt 

7. topN: the number of top-k in Top-K application (default 10)
  - You don't need to set if you don't want to run Top-k application 

8. runtime: runtime (sec) 
  - It doesn't need in cluster mode. In cluster mode, we should kill by hand.

9. input: input path (default: input/test_input) 
  - It doesn't need in RandomWordSpout. 

10. spout: FileReadWordSpout / RandomWordSpout 
  - FileReadWordSpout: read input from path of input parameter.
  - RandomWordSpout: create random word input

11. timescales: timescale information 
  - format: (\\(\\d+,\\d+\\))*
  - ex) timescales=(30,2)(50,4)

12. output: output path
  - It logs latency and throughput. 
