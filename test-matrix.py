#!/usr/bin/env python3
"""运行 Hadoop Streaming 矩阵乘法测试"""

from __future__ import annotations

import array
import base64
import json
import platform
import random
import re
import shlex
import struct
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import json5


BASE_DIR = Path(__file__).resolve().parent
BEIJING = ZoneInfo("Asia/Shanghai")


def log(message: str) -> None:
    print(f"[{datetime.now(BEIJING):%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def progress(message: str) -> None:
    sys.stdout.write(f"\r\033[2K[{datetime.now(BEIJING):%Y-%m-%d %H:%M:%S}] {message}")
    sys.stdout.flush()


def known_hosts_option(private_key: str) -> str:
    path = f"{private_key}.known_hosts".replace("\\", "\\\\").replace('"', '\\"')
    return f'UserKnownHostsFile="{path}"'


def ssh(node: dict, user: str, key_path: str, command: str, timeout: int = 600):
    """通过部署密钥执行远程命令"""
    return subprocess.run([
        "ssh", "-i", key_path, "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", known_hosts_option(key_path),
        "-o", "StrictHostKeyChecking=accept-new",
        f"{user}@{node.get('public_ip') or node['private_ip']}", command,
    ], text=True, capture_output=True, timeout=timeout, check=True)


def copy_to(node: dict, user: str, key_path: str, source: Path, destination: str) -> None:
    """通过 SCP 上传文件"""
    subprocess.run([
        "scp", "-i", key_path, "-o", "BatchMode=yes",
        "-o", known_hosts_option(key_path),
        "-o", "StrictHostKeyChecking=accept-new", str(source),
        f"{user}@{node.get('public_ip') or node['private_ip']}:{destination}",
    ], check=True, timeout=300)


def make_hadoop_input(n: int, block: int, rng: random.Random, work: Path) -> tuple[Path, Path]:
    """生成本地校验矩阵和 Hadoop Streaming 分块输入"""
    a = array.array("f", (rng.uniform(-1.0, 1.0) for _ in range(n * n)))
    b = array.array("f", (rng.uniform(-1.0, 1.0) for _ in range(n * n)))
    local_file = work / f"matrix-{n}.bin"
    with local_file.open("wb") as output:
        output.write(struct.pack("i", n))
        a.tofile(output)
        b.tofile(output)
    input_file = work / f"blocks-{n}.txt"
    cells = block * block
    with input_file.open("w", encoding="ascii") as output:
        for i in range(0, n, block):
            for j in range(0, n, block):
                for k in range(0, n, block):
                    values = array.array("f", [0.0]) * (2 * cells)
                    for row in range(block):
                        if i + row < n:
                            start = (i + row) * n + k
                            values[row * block:row * block + block] = a[start:start + block]
                        if k + row < n:
                            start = (k + row) * n + j
                            offset = cells + row * block
                            values[offset:offset + block] = b[start:start + block]
                    payload = base64.b64encode(values.tobytes()).decode("ascii")
                    output.write(f"{i // block} {j // block} {k // block}\t{payload}\n")
    return local_file, input_file


def compile_verifier(source: Path, executable: Path) -> None:
    """编译仅用于结果校验的本地 CBLAS 程序"""
    command = ["c++", "-O3", "-std=c++17", str(source), "-o", str(executable)]
    if platform.system() == "Darwin":
        command += ["-Wno-deprecated-declarations", "-framework", "Accelerate"]
    else:
        command += ["-lopenblas"]
    subprocess.run(command, check=True)


def write_result(path: Path, data: list[dict]) -> None:
    """保存已完成规格的 Hadoop 耗时"""
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    """生成矩阵分块输入、提交 Hadoop 作业并记录耗时"""
    config = json5.loads((BASE_DIR / "test-matrix-config.jsonc").read_text(encoding="utf-8"))
    matrix_sizes = config["matrix_sizes"]
    repeats = int(config["repeats"])
    timeout_seconds = int(config["timeout_seconds"])
    test_cases = config["test_cases"]
    selected_indices = set(map(int, config.get("test_case_indices", range(1, len(test_cases) + 1))))
    selected_cases = [
        (index, case) for index, case in enumerate(test_cases, start=1)
        if index in selected_indices
    ]
    state = json.loads((BASE_DIR / "deployment-state.json").read_text(encoding="utf-8"))
    expiry = datetime.fromisoformat(state["expires_at"].replace("Z", "+00:00"))
    if expiry <= datetime.now(timezone.utc):
        raise RuntimeError("实例已到自动释放时间")
    nodes = state["nodes"]
    master = nodes[0]
    user = state.get("ssh_user", "root")
    key_path = str(Path(state["ssh_private_key_file"]).expanduser())
    home = state.get("hadoop_home", "/usr/local/hadoop")
    stamp = datetime.now(BEIJING).strftime("%Y%m%d-%H%M%S")
    work = BASE_DIR / f"matrix-results-{stamp}"
    work.mkdir()
    source = BASE_DIR / "matrix-multiply.cpp"
    verifier = work / "matrix-verifier"
    compile_verifier(source, verifier)
    log(f"C++ 结果校验程序：{source}")
    log(f"Hadoop 测试规格：{matrix_sizes}，运行测试项 {[index for index, _ in selected_cases]}，每种重复 {repeats} 次，超时 {timeout_seconds} 秒")

    # 在主节点编译可分发的 Hadoop 程序
    log("上传源码并编译 Hadoop 程序……")
    remote_root = f"/tmp/hadoop-matrix-{stamp}"
    remote_script = "/tmp/run-hadoop-matrix.sh"
    copy_to(master, user, key_path, BASE_DIR / "run-hadoop-matrix.sh", remote_script)
    copy_to(master, user, key_path, source, f"{remote_root}.cpp")
    ssh(master, user, key_path, f"HADOOP_HOME={shlex.quote(home)} bash {remote_script} compile {remote_root}.cpp {remote_root}")

    hdfs_root = f"/user/hadoop/matrix-{stamp}"
    ssh(master, user, key_path, f"HADOOP_HOME={shlex.quote(home)} bash {remote_script} setup {shlex.quote(hdfs_root)}")

    rng = random.Random(20260929)
    report = BASE_DIR / "result.json"
    result_data = []
    write_result(report, result_data)
    for case_index, case in selected_cases:
        num_reduce_tasks = int(case["num_reduce_tasks"])
        split_max_size = int(case["split_max_size"])
        block_size = int(case["block_size"])
        log(f"测试项 {case_index}/{len(test_cases)}：Reduce {num_reduce_tasks} 个，切片 {split_max_size} 字节，分块 {block_size}×{block_size}")
        for n in matrix_sizes:
            log(f"矩阵规格 {n}×{n}：生成 Hadoop 分块输入……")
            local_input, hdfs_input = make_hadoop_input(n, block_size, rng, work)
            local_result = work / f"local-result-{case_index}-{n}.bin"
            subprocess.run([
                str(verifier), "verify", str(local_input), str(local_result), str(n),
            ], check=True)
            remote_reference = f"{remote_root}-reference-{case_index}-{n}.bin"
            copy_to(master, user, key_path, local_result, remote_reference)
            hdfs_input_dir = f"{hdfs_root}/case-{case_index}/input-{n}"
            remote_input = f"{remote_root}-input-{case_index}-{n}.txt"
            copy_to(master, user, key_path, hdfs_input, remote_input)
            ssh(master, user, key_path, (
                f"HADOOP_HOME={shlex.quote(home)} bash {remote_script} put "
                f"{shlex.quote(remote_input)} {shlex.quote(hdfs_input_dir)}"
            ))
            hdfs_stats = ssh(master, user, key_path, (
                f"HADOOP_HOME={shlex.quote(home)} bash {remote_script} stats "
                f"{shlex.quote(hdfs_input_dir)}"
            ))
            hdfs_block_count = int(re.search(r"HDFS_BLOCK_COUNT=([0-9]+)", hdfs_stats.stdout).group(1))

            times = []
            map_task_counts = []
            reduce_task_counts = []
            map_input_record_counts = []
            map_output_record_counts = []
            shuffle_byte_counts = []
            node_counts = []
            final_output = ""
            for repeat in range(repeats):
                output_dir = f"{hdfs_root}/case-{case_index}/output-{n}-{repeat}"
                final_output = output_dir
                progress(f"测试项 {case_index}，{n}×{n} 作业 {repeat + 1}/{repeats}……")
                run = ssh(master, user, key_path, (
                    f"HADOOP_HOME={shlex.quote(home)} bash {remote_script} job "
                    f"{shlex.quote(remote_root)} {shlex.quote(hdfs_input_dir)} "
                    f"{shlex.quote(output_dir)} {n} {repeat} {block_size} "
                    f"{split_max_size} {num_reduce_tasks} {timeout_seconds}"
                ), timeout=600)
                elapsed = float(re.search(r"JOB_SECONDS=([0-9.]+)", run.stdout).group(1))
                map_tasks = int(re.search(r"MAP_TASKS=([0-9]+)", run.stdout).group(1))
                reduce_tasks = int(re.search(r"REDUCE_TASKS=([0-9]+)", run.stdout).group(1))
                map_input_records = int(re.search(r"MAP_INPUT_RECORDS=([0-9]+)", run.stdout).group(1))
                map_output_records = int(re.search(r"MAP_OUTPUT_RECORDS=([0-9]+)", run.stdout).group(1))
                shuffle_bytes = int(re.search(r"SHUFFLE_BYTES=([0-9]+)", run.stdout).group(1))
                nodes_used = int(re.search(r"NODES_USED=([0-9]+)", run.stdout).group(1))
                times.append(elapsed)
                map_task_counts.append(map_tasks)
                reduce_task_counts.append(reduce_tasks)
                map_input_record_counts.append(map_input_records)
                map_output_record_counts.append(map_output_records)
                shuffle_byte_counts.append(shuffle_bytes)
                node_counts.append(nodes_used)
                progress(f"测试项 {case_index}，{n}×{n} 作业 {repeat + 1}/{repeats} 完成，耗时 {elapsed:.6f} 秒")
                print(flush=True)

            remote_result = f"{remote_root}-result-{case_index}-{n}.txt"
            ssh(master, user, key_path, (
                f"HADOOP_HOME={shlex.quote(home)} bash {remote_script} getmerge "
                f"{shlex.quote(final_output)} {shlex.quote(remote_result)}"
            ))
            checked = ssh(master, user, key_path, (
                f"HADOOP_HOME={shlex.quote(home)} bash {remote_script} compare "
                f"{shlex.quote(remote_root)} {shlex.quote(remote_reference)} "
                f"{shlex.quote(remote_result)} {n} {block_size}"
            ))
            difference = float(re.search(r"MAX_ABS_ERROR=([0-9.eE+-]+)", checked.stdout).group(1))
            log(f"测试项 {case_index}，{n}×{n} 云端结果校验通过，最大误差 {difference:.6g}")
            result_data.append({
                "case_index": case_index,
                "size": n,
                "num_reduce_tasks": num_reduce_tasks,
                "split_max_size": split_max_size,
                "block_size": block_size,
                "hdfs_block_count": hdfs_block_count,
                "map_input_records": map_input_record_counts,
                "map_output_records": map_output_record_counts,
                "shuffle_bytes": shuffle_byte_counts,
                "map_tasks": map_task_counts,
                "reduce_tasks": reduce_task_counts,
                "nodes_used": node_counts,
                "times": [f"{seconds:.6f}" for seconds in times],
            })
            write_result(report, result_data)
            log(f"测试项 {case_index}，{n}×{n} Hadoop 结果已追加到 result.json")

    log(f"测试完成")


if __name__ == "__main__":
    main()
