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
    cat > "$3-map.sh" <<'EOF'
#!/usr/bin/env bash
echo "reporter:counter:TaskNodes,$(hostname),1" >&2
exec ./matrix map "$1"
EOF
    cat > "$3-reduce.sh" <<'EOF'
#!/usr/bin/env bash
echo "reporter:counter:TaskNodes,$(hostname),1" >&2
exec ./matrix reduce "$1"
EOF
    chmod 755 "$3"
    chmod 755 "$3-map.sh" "$3-reduce.sh"
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

# 统计 HDFS 输入文件的逻辑分块数量
if [ "$ACTION" = "stats" ]; then
    run_hadoop "$HADOOP_HOME/bin/hdfs fsck '$2' -files -blocks" \
        | grep -oE '[0-9]+ block(\(s\))?' \
        | awk '{ total += $1 } END { print "HDFS_BLOCK_COUNT=" (total + 0) }'
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
    JOB_LOG="/tmp/hadoop-matrix-job-$$.log"
    START="$(date +%s%N)"
    run_hadoop "timeout '$TIMEOUT_SECONDS' '$HADOOP_HOME/bin/hadoop' jar '$STREAMING_JAR' \
        -D mapreduce.job.name=matrix-'$SIZE'-'$REPEAT' \
        -D mapreduce.input.fileinputformat.split.maxsize='$SPLIT_MAX_SIZE' \
        -files '$REMOTE_ROOT'#matrix,'$REMOTE_ROOT-map.sh'#matrix-map,'$REMOTE_ROOT-reduce.sh'#matrix-reduce \
        -cmdenv OPENBLAS_NUM_THREADS=1 \
        -numReduceTasks '$NUM_REDUCE_TASKS' -input '$INPUT_DIR' -output '$OUTPUT_DIR' \
        -mapper './matrix-map '$BLOCK_SIZE'' \
        -reducer './matrix-reduce '$BLOCK_SIZE'' > '$JOB_LOG' 2>&1"
    END="$(date +%s%N)"
    python3 -c 'import sys; print("JOB_SECONDS=%.6f" % ((int(sys.argv[2])-int(sys.argv[1]))/1e9))' "$START" "$END"
    MAP_TASKS="$(sed -n 's/.*Launched map tasks=\([0-9][0-9]*\).*/\1/p' "$JOB_LOG" | tail -1)"
    REDUCE_TASKS="$(sed -n 's/.*Launched reduce tasks=\([0-9][0-9]*\).*/\1/p' "$JOB_LOG" | tail -1)"
    MAP_INPUT_RECORDS="$(sed -n 's/.*Map input records=\([0-9][0-9]*\).*/\1/p' "$JOB_LOG" | tail -1)"
    MAP_OUTPUT_RECORDS="$(sed -n 's/.*Map output records=\([0-9][0-9]*\).*/\1/p' "$JOB_LOG" | tail -1)"
    SHUFFLE_BYTES="$(sed -n 's/.*Reduce shuffle bytes=\([0-9][0-9]*\).*/\1/p' "$JOB_LOG" | tail -1)"
    APPLICATION_ID="$(sed -n 's/.*Submitted application \(application_[0-9_]*\).*/\1/p' "$JOB_LOG" | tail -1)"
    NODES_USED="$(awk '
        /^[[:space:]]+TaskNodes[[:space:]]*$/ {
            match($0, /^[[:space:]]*/)
            group_indent = RLENGTH
            inside = 1
            next
        }
        inside {
            match($0, /^[[:space:]]*/)
            if (RLENGTH <= group_indent) exit
            if ($0 ~ /^[[:space:]]+[A-Za-z0-9._-]+=[0-9]+[[:space:]]*$/) count++
        }
        END { print count + 0 }
    ' "$JOB_LOG")"
    printf 'MAP_TASKS=%s\nREDUCE_TASKS=%s\nMAP_INPUT_RECORDS=%s\nMAP_OUTPUT_RECORDS=%s\nSHUFFLE_BYTES=%s\nNODES_USED=%s\n' \
        "$MAP_TASKS" "$REDUCE_TASKS" "$MAP_INPUT_RECORDS" "$MAP_OUTPUT_RECORDS" "$SHUFFLE_BYTES" "$NODES_USED"
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
