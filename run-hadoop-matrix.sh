#!/usr/bin/env bash
set -euo pipefail

# 读取 Hadoop 安装目录和操作类型
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
ACTION="$1"

# 以 hadoop 用户执行 Hadoop 命令
run_hadoop() {
    local command="$1"
    local java_home
    java_home="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
    runuser -u hadoop -- env \
        JAVA_HOME="$java_home" \
        HADOOP_HOME="$HADOOP_HOME" \
        HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop" \
        PATH="$java_home/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH" \
        bash -c "cd /tmp && $command"
}

# 编译 Hadoop Streaming 使用的矩阵程序
if [ "$ACTION" = "compile" ]; then
    g++ -O3 -std=c++17 "$2" -lopenblas -o "$3"
    chmod 755 "$3"
    exit 0
fi

# 查找 Hadoop Streaming 包
STREAMING_JAR="$(find "$HADOOP_HOME/share/hadoop/tools/lib" -maxdepth 1 -name 'hadoop-streaming-*.jar' -print -quit)"

# 创建 HDFS 测试根目录
if [ "$ACTION" = "setup" ]; then
    run_hadoop "$HADOOP_HOME/bin/hdfs dfs -mkdir -p '$2'"
    exit 0
fi

# 把主节点临时文件写入 HDFS
if [ "$ACTION" = "put" ]; then
    run_hadoop "$HADOOP_HOME/bin/hdfs dfs -mkdir -p '$3' && $HADOOP_HOME/bin/hdfs dfs -put '$2' '$3/'"
    exit 0
fi

# 提交一次矩阵乘法 MapReduce 作业并输出耗时
if [ "$ACTION" = "job" ]; then
    REMOTE_ROOT="$2"
    INPUT_DIR="$3"
    OUTPUT_DIR="$4"
    SIZE="$5"
    REPEAT="$6"
    BLOCK_SIZE="$7"
    SPLIT_MAX_SIZE="$8"
    NUM_REDUCE_TASKS="$9"
    TIMEOUT_SECONDS="${10}"
    START="$(date +%s%N)"
    run_hadoop "timeout '$TIMEOUT_SECONDS' '$HADOOP_HOME/bin/hadoop' jar '$STREAMING_JAR' \
        -D mapreduce.job.name=matrix-'$SIZE'-'$REPEAT' \
        -D mapreduce.input.fileinputformat.split.maxsize='$SPLIT_MAX_SIZE' \
        -files '$REMOTE_ROOT'#matrix -cmdenv OPENBLAS_NUM_THREADS=1 \
        -numReduceTasks '$NUM_REDUCE_TASKS' -input '$INPUT_DIR' -output '$OUTPUT_DIR' \
        -mapper './matrix map '$BLOCK_SIZE'' -reducer './matrix reduce '$BLOCK_SIZE''"
    END="$(date +%s%N)"
    python3 -c 'import sys; print("JOB_SECONDS=%.6f" % ((int(sys.argv[2])-int(sys.argv[1]))/1e9))' "$START" "$END"
    exit 0
fi

# 从 HDFS 合并结果文件到主节点本地
if [ "$ACTION" = "getmerge" ]; then
    run_hadoop "$HADOOP_HOME/bin/hdfs dfs -getmerge '$2' '$3'"
    exit 0
fi

# 比较 Hadoop 结果和本地参考结果
if [ "$ACTION" = "compare" ]; then
    run_hadoop "$2 compare '$3' '$4' '$5' '$6'"
    exit 0
fi

exit 2
