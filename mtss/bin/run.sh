#!/bin/sh

#CMD="mvn clean install"
#echo $CMD
#$CMD

SELF_JAR='./target/mtss-1.0-SNAPSHOT.jar'
TARGET_CLASS='edu.snu.org.WordCountApp'

CMD="java -cp ::$SELF_JAR $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $TARGET_CLASS `cat $1`"
echo $CMD
$CMD

