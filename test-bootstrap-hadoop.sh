#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_SCRIPT="$SCRIPT_DIR/bootstrap-hadoop-node.sh"
TEST_HOME="${TEST_HOME:-/tmp/hadoop-bootstrap-test}"
LOG_FILE="$TEST_HOME/bootstrap.log"
RESULT_FILE="$TEST_HOME/wordcount-result.txt"
HADOOP_VERSION="3.4.2"

mkdir -p "$TEST_HOME"
exec > >(tee "$LOG_FILE") 2>&1

bash -n "$NODE_SCRIPT"
REPLICATION=1 bash "$NODE_SCRIPT" master localhost /dev/null
JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"

hdfs() { runuser -u hadoop -- env JAVA_HOME="$JAVA_HOME" HADOOP_HOME=/usr/local/hadoop HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop PATH="$JAVA_HOME/bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin" /usr/local/hadoop/bin/hdfs "$@"; }
hadoop() { runuser -u hadoop -- env JAVA_HOME="$JAVA_HOME" HADOOP_HOME=/usr/local/hadoop HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop PATH="$JAVA_HOME/bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin" /usr/local/hadoop/bin/hadoop "$@"; }

for attempt in $(seq 1 30); do
    if hdfs dfsadmin -report >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

HDFS_TEST_DIR="/tmp/hadoop-bootstrap-wordcount-$$"
LOCAL_INPUT="$TEST_HOME/input.txt"
printf 'hadoop hadoop mapreduce\nmapreduce hadoop\n' > "$LOCAL_INPUT"
hdfs dfs -mkdir -p "$HDFS_TEST_DIR/input"
hdfs dfs -put -f "$LOCAL_INPUT" "$HDFS_TEST_DIR/input/input.txt"
hadoop jar "/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar" wordcount "$HDFS_TEST_DIR/input" "$HDFS_TEST_DIR/output"
hdfs dfs -cat "$HDFS_TEST_DIR/output/part-r-*" | tee "$RESULT_FILE"
awk '$1 == "hadoop" && $2 == 3 { h=1 } $1 == "mapreduce" && $2 == 2 { m=1 } END { exit !(h && m) }' "$RESULT_FILE"
hdfs dfsadmin -report

printf '测试通过：JDK、HDFS 和 MapReduce WordCount\n'
