#!/bin/bash

if [ -z "$BASE_DIR" ] ; then
    PRG="$0"

    # need this for relative symlinks
    while [ -h "$PRG" ] ; do
        ls=`ls -ld "$PRG"`
        link=`expr "$ls" : '.*-> \(.*\)$'`
        if expr "$link" : '/.*' > /dev/null; then
            PRG="$link"
        else
            PRG="`dirname "$PRG"`/$link"
        fi
    done
    BASE_DIR=`dirname "$PRG"`/..

    # make it fully qualified
    BASE_DIR=`cd "$BASE_DIR" && pwd`
    #echo "collect master is at $BASE_DIR"
fi

source $BASE_DIR/bin/env.sh

AS_USER=`whoami`
LOG_DIR="$BASE_DIR/logs"
LOG_FILE="$LOG_DIR/server.log"
PID_FILE="$BASE_DIR/.pid"
JMX_PORT_FILE="$BASE_DIR/.jmx_port"

HOST_NAME=`hostname`

#判断当前端口是否被占用，没被占用返回0，反之1
function Listening() {
   TCPListeningnum=`netstat -an | grep ":$1 " | awk '$1 == "tcp" && $NF == "LISTEN" {print $0}' | wc -l`
   UDPListeningnum=`netstat -an | grep ":$1 " | awk '$1 == "udp" && $NF == "0.0.0.0:*" {print $0}' | wc -l`
   (( Listeningnum = TCPListeningnum + UDPListeningnum ))
   if [ $Listeningnum == 0 ]; then
       echo "0"
   else
       echo "1"
   fi
}

#指定区间随机数
function random_range() {
   shuf -i $1-$2 -n1
}

#得到随机端口
JMX_PORT=0
function get_random_port() {
   templ=0
   while [ $JMX_PORT == 0 ]; do
       temp1=`random_range $1 $2`
       if [ `Listening $temp1` == 0 ] ; then
              JMX_PORT=$temp1
       fi
   done
}

if [ $1 == "start" ]; then
  get_random_port 4000 5000
  echo $JMX_PORT > $JMX_PORT_FILE
fi

function running(){
    if [ -f "$PID_FILE" ]; then
        pid=$(cat "$PID_FILE")
        process=`ps aux | grep " $pid " | grep -v grep`;
        if [ "$process" == "" ]; then
            return 1;
        else
            return 0;
        fi
    else
        return 1
    fi
}

function start_server() {
	  if running; then
		    echo "$SERVER_NAME is running."
		    exit 1
	  fi

    mkdir -p $LOG_DIR

    touch $LOG_FILE
    chown -R $AS_USER $LOG_DIR

    echo "$JAVA $APP_JVM_ARGS -DBASE_HOME=$BASE_HOME -DSERVER_NAME=$SERVER_NAME-$HOST_NAME -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
   	  -Dcom.sun.management.jmxremote.port=$JMX_PORT $BASE_APP_ARGS $APP_START_MAIN_CLASS"
    sleep 1
    nohup $JAVA $APP_JVM_ARGS -DBASE_HOME=$BASE_HOME -DSERVER_NAME=$SERVER_NAME-$HOST_NAME -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false \
   	  -Dcom.sun.management.jmxremote.port=$JMX_PORT $BASE_APP_ARGS $APP_START_MAIN_CLASS 2>&1 >>$LOG_FILE &
    echo $! > $PID_FILE

    chmod 755 $PID_FILE

    echo "输出日志文件位置: $HOME/output/datainsight/logs"
    echo "查询应用运行状态，执行命令: ./bin/server.sh status"
}

function stop_server() {
    if ! running; then
        echo "$SERVER_NAME is not running."
        exit 1
    fi
    count=0
    pid=$(cat $PID_FILE)
    jmx_port=$(cat $JMX_PORT_FILE)
    echo "jmx port $jmx_port"

    while running;
    do
        let count=$count+1
        # echo "Stopping $SERVER_NAME $count times"
        if [ $count -gt 3 ]; then
            echo "kill -9 $pid"
            kill -9 $pid
        else
            sleep 1
            $JAVA $APP_JVM_ARGS -DBASE_HOME=$BASE_HOME -Dhost=127.0.0.1 -Dport=$jmx_port $APP_STOP_MAIN_CLASS $@
        fi
        sleep 3;
    done

    # echo "Stop $SERVER_NAME successfully."
    rm $PID_FILE
}

function print_log() {
    properties="$BASE_DIR/conf/application.properties"
    keyToSearch="spring.profiles.active"
    profile="$(grep "$keyToSearch" "$properties" | cut -d'=' -f2-)"

    echo "profile: $profile"
    if [ $profile == "production" ] || [ $profile == "test" ]; then
        tail -200f ~/output/datawork/logs/datawork.log
    elif [ $profile == "dev" ]; then
        tail -f $LOG_FILE
    else
        echo "profile 不正确: $profile";
    fi
}

function status(){
    if running; then
        echo "$SERVER_NAME is running.";
        exit 0;
    else
        echo "$SERVER_NAME was stopped.";
        exit 1;
    fi
}

function help() {
    echo "Usage: server.sh {start|status|stop|restart}" >&2
    echo "       start:             start the $SERVER_NAME server"
    echo "       stop:              stop the $SERVER_NAME server"
    echo "       restart:           restart the $SERVER_NAME server"
    echo "       status:            get $SERVER_NAME current status,running or stopped."
    echo "       log:               print $SERVER_NAME log."
}

command=$1
shift 1
case $command in
    start)
        start_server $@;
        ;;
    stop)
        stop_server $@;
        ;;
    log)
        print_log $@;
        ;;
    status)
    	  status $@;
        ;;
    restart)
        $0 stop $@
        $0 start $@
        ;;
    help)
        help;
        ;;
    *)
        help;
        exit 1;
        ;;
esac
