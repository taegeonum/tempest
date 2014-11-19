#CMD="mvn clean install"
#echo $CMD
#$CMD

SELF_JAR='./target/mtss-1.0-SNAPSHOT-jar-with-dependencies.jar'
TARGET_CLASS='edu.snu.org.WordcountApp'

CMD="java -cp ::$SELF_JAR $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $TARGET_CLASS $*"
echo $CMD
$CMD

TARGET_CLASS='edu.snu.org.WordcountAppNaive'
CMD="java -cp ::$SELF_JAR $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $TARGET_CLASS $*"
echo $CMD
$CMD
