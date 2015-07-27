#!/bin/sh

#CMD="mvn clean install"
#echo $CMD
#$CMD

SELF_JAR='./target/mtss-1.0-SNAPSHOT.jar'
TARGET_CLASS='edu.snu.org.WordCountApp'

CMD="storm jar $SELF_JAR $TARGET_CLASS `cat $1`"
echo $CMD
$CMD

