#!/usr/bin/env python3
"""检查 SSH 连通性、HDFS 和 MapReduce WordCount 作业。"""

from __future__ import annotations

import json
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def ssh_endpoint(node: dict) -> str:
    """优先使用公网地址连接节点，否则使用内网地址。"""
    return node.get("public_ip") or node["private_ip"]


def ssh(node: dict, key_path: str, command: str, timeout: int = 30):
    """使用状态文件中的用户和私钥执行 SSH 命令。"""
    host = ssh_endpoint(node)
    user = node.get("ssh_user", "root")
    return subprocess.run([
        "ssh", "-i", key_path,
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=8",
        "-o", f"UserKnownHostsFile={key_path}.known_hosts",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ServerAliveInterval=8",
        f"{user}@{host}", command,
    ], text=True, capture_output=True, timeout=timeout, check=True)


def hadoop_command(command: str) -> str:
    """构造以 hadoop 用户和固定安装路径运行的命令。"""
    return 'JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")"); runuser -u hadoop -- env JAVA_HOME="$JAVA_HOME" HADOOP_HOME=/usr/local/hadoop HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop PATH="$JAVA_HOME/bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:$PATH" ' + command


def expiry_guard(state: dict) -> None:
    """检查云端自动释放时间，过期后停止提交测试任务。"""
    expiry = datetime.fromisoformat(state["expires_at"].replace("Z", "+00:00"))
    remaining = (expiry - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        raise RuntimeError("部署状态已到达服务端自动释放时间；请勿继续向实例提交作业")
    print(f"实例剩余有效时间约 {int(remaining // 60)} 分钟")


def check_connectivity(nodes: list[dict], key_path: str) -> None:
    """检查主节点到所有节点及全节点环路的内网解析和 SSH 互信。"""
    master = nodes[0]
    targets = [(master, node) for node in nodes]
    targets += [(node, nodes[(index + 1) % len(nodes)]) for index, node in enumerate(nodes)]

    def check_pair(pair):
        """从一个节点检查到另一个节点的名称解析和 SSH 登录。"""
        source, target = pair
        remote_target = target["private_ip"]
        command = (
            "getent hosts " + shlex.quote(target["name"]) + " >/dev/null 2>&1 "
            "&& ssh -o BatchMode=yes -o ConnectTimeout=6 -o StrictHostKeyChecking=accept-new "
            + shlex.quote(remote_target) + " true"
        )
        return source, target, ssh(source, key_path, command, timeout=20)

    print("SSH 与节点名解析检查（主节点覆盖全部节点，并抽查全节点环路）：")
    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = [pool.submit(check_pair, pair) for pair in targets]
        for future in as_completed(futures):
            source, target, _ = future.result()
            print(f"  {source['name']} -> {target['name']}: 通过")


def hadoop_test(state: dict, key_path: str, master: dict) -> int:
    """向 HDFS 写入样例文本，运行 WordCount 并核对词频结果。"""
    hadoop_home = shlex.quote(state.get("hadoop_home", "/usr/local/hadoop"))
    script = f'''set -eu
root="/tmp/hadoop-lab-check-$$"
local_file="/tmp/hadoop-lab-check-$$.txt"
JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")
HDFS="runuser -u hadoop -- env JAVA_HOME=$JAVA_HOME HADOOP_HOME={hadoop_home} HADOOP_CONF_DIR={hadoop_home}/etc/hadoop PATH=$JAVA_HOME/bin:{hadoop_home}/bin:{hadoop_home}/sbin:$PATH {hadoop_home}/bin/hdfs"
HADOOP="runuser -u hadoop -- env JAVA_HOME=$JAVA_HOME HADOOP_HOME={hadoop_home} HADOOP_CONF_DIR={hadoop_home}/etc/hadoop PATH=$JAVA_HOME/bin:{hadoop_home}/bin:{hadoop_home}/sbin:$PATH {hadoop_home}/bin/hadoop"
printf 'hadoop hadoop mapreduce\\nmapreduce hadoop\\n' > "$local_file"
eval "$HDFS dfs -mkdir -p \"$root/input\""
eval "$HDFS dfs -put \"$local_file\" \"$root/input/input.txt\""
examples_jar=$(find {hadoop_home}/share/hadoop/mapreduce -maxdepth 1 -name 'hadoop-mapreduce-examples-*.jar' -print -quit)
test -n "$examples_jar"
eval "$HADOOP jar \"$examples_jar\" wordcount \"$root/input\" \"$root/output\""
result=$(eval "$HDFS dfs -cat \"$root/output/part-r-*\"")
printf '%s\\n' "$result"
printf '%s\\n' "$result" | awk '$1 == "hadoop" && $2 == 3 {{ h=1 }} $1 == "mapreduce" && $2 == 2 {{ m=1 }} END {{ exit !(h && m) }}'
$HDFS dfs -rm -r -f "$root" >/dev/null
rm -f "$local_file"
'''
    print("运行 Hadoop 示例 WordCount：")
    result = subprocess.run([
        "ssh", "-i", key_path, "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", f"UserKnownHostsFile={key_path}.known_hosts",
        "-o", "StrictHostKeyChecking=accept-new", f"{master.get('ssh_user', 'root')}@{ssh_endpoint(master)}",
        "bash -s",
    ], input=script, text=True, capture_output=True, timeout=300, check=True)
    print("  作业成功，输出词频正确：")
    print("  " + "\n  ".join(result.stdout.strip().splitlines()[-8:]))


if __name__ == "__main__":
    # 读取部署生成的状态文件，获取节点列表和 SSH 私钥路径。
    state = json.loads((BASE_DIR / "deployment-state.json").read_text(encoding="utf-8"))
    nodes = state["nodes"]
    key_path = str(Path(state["ssh_private_key_file"]).expanduser())
    # 确认实例尚未到期，并检查节点间名称解析和 SSH 互信。
    expiry_guard(state)
    check_connectivity(nodes, key_path)
    # 选择主节点，通过 HDFS 管理命令读取集群状态。
    master = nodes[0]
    hdfs = ssh(master, key_path,
               hadoop_command("/usr/local/hadoop/bin/hdfs dfsadmin -report"), timeout=45)
    print("HDFS 报告摘要：")
    print("\n".join(hdfs.stdout.strip().splitlines()[:14]))
    # 提交 WordCount 作业并校验两个单词的预期词频。
    hadoop_test(state, key_path, master)
    # 所有检查成功后输出最终结果。
    print("测试通过：SSH、HDFS、MapReduce WordCount")
