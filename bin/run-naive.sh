CMD="mvn clean install"
echo $CMD
$CMD

SELF_JAR='./target/mtss-1.0-SNAPSHOT-jar-with-dependencies.jar'
TARGET_CLASS='edu.snu.org.naive.NaiveWordCountTopology'

LOGGING_CONFIG='-Djava.util.logging.config.class=com.microsoft.reef.util.logging.Config'
CMD="java -cp ::$SELF_JAR $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $TARGET_CLASS $*"
echo $CMD
$CMD
