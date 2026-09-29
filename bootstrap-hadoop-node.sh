#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-worker}"
MASTER_HOST="${2:-hadoop-lab-001}"
WORKERS_FILE="${3:-/tmp/hadoop-workers}"
REPLICATION="${REPLICATION:-3}"
HADOOP_HOME="/usr/local/hadoop"
RUN_USER="${RUN_USER:-hadoop}"
CACHE_DIR="/var/cache/hadoop-bootstrap"
ARTIFACT_PORT="8765"

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl openssh-client openjdk-8-jdk
JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
id -u "$RUN_USER" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$RUN_USER"

mkdir -p "$CACHE_DIR"
if [ "$ROLE" = "master" ]; then
    if [ ! -s "$CACHE_DIR/hadoop.tar.gz" ]; then
        curl -fL "https://mirrors.aliyun.com/apache/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz" -o "$CACHE_DIR/hadoop.tar.gz"
    fi
    nohup python3 -m http.server "$ARTIFACT_PORT" --bind 0.0.0.0 --directory "$CACHE_DIR" >/var/log/hadoop-artifacts.log 2>&1 </dev/null &
else
    curl -fL "http://${MASTER_HOST}:${ARTIFACT_PORT}/hadoop.tar.gz" -o "$CACHE_DIR/hadoop.tar.gz"
fi

mkdir -p "$HADOOP_HOME"
tar -xzf "$CACHE_DIR/hadoop.tar.gz" -C "$HADOOP_HOME" --strip-components=1

install -d -o "$RUN_USER" -g "$RUN_USER" /var/lib/hadoop/hdfs/namenode /var/lib/hadoop/hdfs/datanode /var/lib/hadoop/tmp /var/log/hadoop
chown -R "$RUN_USER:$RUN_USER" /var/log/hadoop

cat > /etc/profile.d/hadoop.sh <<EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
EOF

cat > "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" <<EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_LOG_DIR=/var/log/hadoop
EOF

cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://$MASTER_HOST:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/var/lib/hadoop/tmp</value></property>
</configuration>
EOF

cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" <<EOF
<configuration>
  <property><name>dfs.replication</name><value>$REPLICATION</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///var/lib/hadoop/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///var/lib/hadoop/hdfs/datanode</value></property>
</configuration>
EOF

cat > "$HADOOP_HOME/etc/hadoop/mapred-site.xml" <<EOF
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>mapreduce.application.classpath</name><value>\$HADOOP_HOME/share/hadoop/mapreduce/*:\$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value></property>
  <property><name>mapreduce.map.memory.mb</name><value>512</value></property>
  <property><name>mapreduce.reduce.memory.mb</name><value>512</value></property>
  <property><name>mapreduce.map.java.opts</name><value>-Xmx384m</value></property>
  <property><name>mapreduce.reduce.java.opts</name><value>-Xmx384m</value></property>
</configuration>
EOF

cat > "$HADOOP_HOME/etc/hadoop/yarn-site.xml" <<EOF
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>$MASTER_HOST</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.app.mapreduce.am.resource.mb</name><value>512</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>1024</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>128</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>1024</value></property>
</configuration>
EOF

install -o "$RUN_USER" -g "$RUN_USER" "$WORKERS_FILE" "$HADOOP_HOME/etc/hadoop/workers"

chown -R "$RUN_USER:$RUN_USER" "$HADOOP_HOME"

if [ "$ROLE" = "master" ]; then
    if [ ! -f /var/lib/hadoop/hdfs/namenode/current/VERSION ]; then
        runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
    fi
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" --daemon start namenode
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" --daemon start datanode
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/yarn" --daemon start resourcemanager
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/yarn" --daemon start nodemanager
elif [ "$ROLE" = "worker" ]; then
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" --daemon start datanode
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/yarn" --daemon start nodemanager
fi
