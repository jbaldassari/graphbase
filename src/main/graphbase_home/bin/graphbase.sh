#!/bin/sh

if [ -z $JAVA_HOME ]; then
	echo "The variable JAVA_HOME must be set to the Java base directory before running this script."
	exit 1
fi

if [ -z $GRAPHBASE_HOME ]; then
	# Try to determine the correct value for GRAPHBASE_HOME:
	cd $(dirname $0)/..
	GRAPHBASE_HOME=$PWD
	export DXAD_HOME
	echo "Warning: GRAPHBASE_HOME is not set.  Using: $GRAPHBASE_HOME"
fi

. $GRAPHBASE_HOME/conf/env.sh

start()
{
	if [ $LOG_GC = true ]; then
		GC_FLAGS="-XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
		if [ $GC_LOG_FILE ]; then
			GC_FLAGS="$GC_FLAGS -Xloggc:$GC_LOG_FILE"
		fi
		JAVA_OPTS="$JAVA_OPTS $GC_FLAGS"
	fi
	
	if [ $JAVA_DEBUG = true ]; then
		echo "Enabling Java debugging"
		if [ ! $JAVA_DEBUG_SUSPEND ]; then
			JAVA_DEBUG_SUSPEND="n"
		fi
		if [ -z $JAVA_DEBUG_PORT ]; then
			JAVA_PROC_COUNT=`ps aux | grep java | grep -v grep | wc -l`
			JAVA_DEBUG_PORT="999$JAVA_PROC_COUNT"
		fi
		echo "Using debug port: $JAVA_DEBUG_PORT"
		PROFILING_DIR=$LOG_DIR/profiling
		mkdir -p $PROFILING_DIR
		HPROF_FILE=$PROFILING_DIR/$$.hprof
		echo "Logging HPROF output to $HPROF_FILE"
		JAVA_OPTS="-Xdebug -agentlib:hprof=file=$HPROF_FILE,format=b -Xrunjdwp:transport=dt_socket,address=$JAVA_DEBUG_PORT,server=y,suspend=$JAVA_DEBUG_SUSPEND $JAVA_OPTS"
	fi

	CLASSPATH="$CLASSPATH":`find $GRAPHBASE_HOME/lib -maxdepth 1 -type f -name "*.jar" -printf "%p:" | sed -e "s/:$//"`
	JAVA_OPTS="$JAVA_OPTS -cp $CLASSPATH"

	exec sh -c "exec $JAVA $JAVA_OPTS graphbase.Graphbase --port $GRAPHBASE_PORT --step $STEP --metricsReloadPeriod $METRICS_RELOAD_SEC --openTsdbConfig $OPENTSDB_CONFIG" 2>&1 > $LOG_DIR/$NAME.out &

	RETVAL=$?
	if [ $RETVAL -eq 0 ]; then
		PID=$! 
		echo "$PID" > $PID_FILE
		echo "Graphbase started (PID $PID)"
	else
		echo "FATAL: Failed to start Graphbase"
	fi
}

stop()
{
	if [ -r $PID_FILE ]; then
		PID=`cat $PID_FILE`
		PROC=`pgrep java | grep $PID`
		if [ -n "$PROC" ]; then
			echo "Killing $DESC (PID $PID)"
			kill $PID
		else
			echo "$DESC not running"
		fi
		rm -f $PID_FILE
	else
		echo "ERROR: $PID_FILE not found or could not be read"
	fi
}

case "$1" in
	start)
		echo "Starting $DESC..."
		start
		;;
		
	stop)
		echo "Stopping $DESC..."
		stop
		;;
	
	*)
		echo "Usage: $0 {start|stop}"
		exit 1
		;;
esac

exit 0

