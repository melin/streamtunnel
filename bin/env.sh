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
APP_START_MAIN_CLASS="com.github.dzlog.DzlogAppMain"

export CLASSPATH=$BASE_DIR/doc:$BASE_DIR/conf:$BASE_DIR/lib/*

#UEAP jvm args
BASE_APP_ARGS=""
BASE_JVM_ARGS_0="-Xmx4g -Xms4g -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=512m -server"
BASE_JVM_ARGS_1="-XX:ReservedCodeCacheSize=256m -XX:+UseCodeCacheFlushing -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled"
BASE_JVM_ARGS_2="-XX:+UseParNewGC -XX:+UseFastAccessorMethods"
BASE_JVM_ARGS_3="-XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 -XX:+HeapDumpOnOutOfMemoryError -Dfile.encoding=UTF-8"
APP_JVM_ARGS="$BASE_JVM_ARGS -cp $CLASSPATH"
