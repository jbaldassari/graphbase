NAME=graphbase
DESC="Graphbase Daemon"
PID_FILE=$GRAPHBASE_HOME/run/$NAME.pid
LOG_DIR=$GRAPHBASE_HOME/logs
LOGBACK_CONFIG=$GRAPHBASE_HOME/conf/logback.xml

JAVA=${JAVA_HOME}/bin/java
JAVA_MAX_HEAP=1G
JAVA_MIN_HEAP=$JAVA_MAX_HEAP
JAVA_OPTS="${JAVA_OPTS} -server -Xms${JAVA_MIN_HEAP} -Xmx${JAVA_MAX_HEAP} -XX:+AggressiveOpts -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSParallelRemarkEnabled -XX:+CMSClassUnloadingEnabled -XX:-OmitStackTraceInFastThrow -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR} -DGRAPHBASE_HOME=$GRAPHBASE_HOME -Dlogback.configurationFile=$LOGBACK_CONFIG"
GC_LOG_FILE=${GRAPHBASE_HOME}/logs/graphbase-gc.`date +'%Y-%m-%d.%H%M'`.log
LOG_GC=false
JAVA_DEBUG=false
CLASSPATH=""	# Add extra libs here

if [ -z "$GRAPHBASE_PORT" ]; then
	GRAPHBASE_PORT=8080
fi
if [ -z "$STEP" ]; then
	STEP=60
fi
if [ -z "$METRICS_RELOAD_SEC" ]; then
	METRICS_RELOAD_SEC=60
fi
if [ -z "$OPENTSDB_CONFIG" ]; then
	OPENTSDB_CONFIG=/etc/opentsdb/opentsdb.conf
fi

