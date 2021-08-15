#!/bin/bash

if [ "$JAVA_HOME" != "" ]; then
  JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
BASE_HOME=$BASE_DIR
SERVER_NAME="datainsight"
APP_START_MAIN_CLASS="com.dataworker.DataWorkerAppMain"
APP_STOP_MAIN_CLASS="com.dataworker.web.support.jmx.StopServerTool"

export CLASSPATH=$BASE_DIR/doc:$BASE_DIR/conf:$BASE_DIR/lib/*

#UEAP jvm args
BASE_APP_ARGS=""
BASE_JVM_ARGS="-Xmx4g -Xms4g -server"
APP_JVM_ARGS="$BASE_JVM_ARGS -cp $CLASSPATH"
