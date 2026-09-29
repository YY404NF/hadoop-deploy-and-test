#!/usr/bin/env bash
set -euo pipefail

# 读取节点角色、主节点地址、工作节点清单和运行参数。
ROLE="${1:-worker}"
MASTER_HOST="${2:-hadoop-lab-001}"
WORKERS_FILE="${3:-/tmp/hadoop-workers}"
REPLICATION="${REPLICATION:-3}"
HADOOP_HOME="/usr/local/hadoop"
RUN_USER="${RUN_USER:-hadoop}"
CACHE_DIR="/var/cache/hadoop-bootstrap"
ARTIFACT_PORT="8765"

# 更新系统软件源并安装 Java、下载和 SSH 所需工具。
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl openssh-client openjdk-8-jdk
JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
# 创建 Hadoop 专用运行用户（仅在用户尚不存在时）。
id -u "$RUN_USER" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$RUN_USER"

# 主节点下载 Hadoop 并提供内网分发；子节点从主节点获取安装包。
mkdir -p "$CACHE_DIR"
if [ "$ROLE" = "master" ]; then
    if [ ! -s "$CACHE_DIR/hadoop.tar.gz" ]; then
        curl -fL "https://mirrors.aliyun.com/apache/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz" -o "$CACHE_DIR/hadoop.tar.gz"
    fi
    nohup python3 -m http.server "$ARTIFACT_PORT" --bind 0.0.0.0 --directory "$CACHE_DIR" >/var/log/hadoop-artifacts.log 2>&1 </dev/null &
else
    curl -fL "http://${MASTER_HOST}:${ARTIFACT_PORT}/hadoop.tar.gz" -o "$CACHE_DIR/hadoop.tar.gz"
fi

# 将 Hadoop 解压到统一安装目录。
mkdir -p "$HADOOP_HOME"
tar -xzf "$CACHE_DIR/hadoop.tar.gz" -C "$HADOOP_HOME" --strip-components=1

# 准备 HDFS、临时数据和日志目录，并设置日志属主。
install -d -o "$RUN_USER" -g "$RUN_USER" /var/lib/hadoop/hdfs/namenode /var/lib/hadoop/hdfs/datanode /var/lib/hadoop/tmp /var/log/hadoop
chown -R "$RUN_USER:$RUN_USER" /var/log/hadoop

# 配置登录 Shell 使用 Java 和 Hadoop 环境变量。
cat > /etc/profile.d/hadoop.sh <<EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
EOF

# 设置 Hadoop 守护进程的 Java 和日志目录。
cat > "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" <<EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_LOG_DIR=/var/log/hadoop
EOF

# 指定 HDFS 默认地址及 Hadoop 临时目录。
cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://$MASTER_HOST:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/var/lib/hadoop/tmp</value></property>
</configuration>
EOF

# 配置副本数以及 NameNode、DataNode 的本地存储目录。
cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" <<EOF
<configuration>
  <property><name>dfs.replication</name><value>$REPLICATION</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///var/lib/hadoop/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///var/lib/hadoop/hdfs/datanode</value></property>
</configuration>
EOF

# 配置 MapReduce 使用 YARN 及容器内存上限。
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

# 配置 ResourceManager 地址、NodeManager 服务和 YARN 内存资源。
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

# 安装工作节点清单并将 Hadoop 文件交给运行用户管理。
install -o "$RUN_USER" -g "$RUN_USER" "$WORKERS_FILE" "$HADOOP_HOME/etc/hadoop/workers"

chown -R "$RUN_USER:$RUN_USER" "$HADOOP_HOME"

# 主节点初始化 HDFS 并启动 NameNode、ResourceManager 和本机工作服务。
if [ "$ROLE" = "master" ]; then
    if [ ! -f /var/lib/hadoop/hdfs/namenode/current/VERSION ]; then
        runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
    fi
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" --daemon start namenode
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" --daemon start datanode
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/yarn" --daemon start resourcemanager
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/yarn" --daemon start nodemanager
# 子节点只启动 DataNode 和 NodeManager。
elif [ "$ROLE" = "worker" ]; then
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/hdfs" --daemon start datanode
    runuser -u "$RUN_USER" -- "$HADOOP_HOME/bin/yarn" --daemon start nodemanager
fi
