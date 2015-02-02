#!/bin/sh

# run WordCount test topology
# parameter: a file path containing test parameters

SELF_JAR='./target/mtss-0.11-SNAPSHOT.jar'
TARGET_CLASS='org.edu.snu.tempest.examples.storm.wordcount.MTSWordCountTestTopology'

CMD="java -cp ::$SELF_JAR $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $TARGET_CLASS `cat $1`"
echo $CMD
$CMD

