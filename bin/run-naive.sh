CMD="mvn clean install"
echo $CMD
$CMD

SELF_JAR='./target/mtss-1.0-SNAPSHOT-jar-with-dependencies.jar'
TARGET_CLASS='edu.snu.org.naive.NaiveWordCountTopology'

CMD="java -cp ::$SELF_JAR $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $TARGET_CLASS $*"
echo $CMD
$CMD
