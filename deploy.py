#!/usr/bin/env python3
"""创建短时 ECS 集群并配置节点间 SSH 互信。"""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


import json5


BASE_DIR = Path(__file__).resolve().parent


def load_config() -> dict[str, Any]:
    return json5.loads((BASE_DIR / "deploy-config.jsonc").read_text(encoding="utf-8"))


def make_client(region: str):
    """读取阿里云密钥并创建指定地域的 ECS 客户端"""
    from alibabacloud_ecs20140526.client import Client
    from alibabacloud_tea_openapi import models

    credentials = json.loads((BASE_DIR / ".aliyun-accessKey").read_text(encoding="utf-8"))
    config = models.Config(
        access_key_id=credentials["AccessKeyId"],
        access_key_secret=credentials["AccessKeySecret"],
        region_id=region,
    )
    config.endpoint = f"ecs.{region}.aliyuncs.com"
    return Client(config)


def make_keypair(config: dict[str, Any]) -> tuple[str, str, str, str]:
    """生成带时间戳的 SSH 密钥对并返回密钥信息"""
    stamp = int(time.time() * 1000)
    private_path = BASE_DIR / f"{config['private_key_file']}-{stamp}"
    public_path = Path(f"{private_path}.pub")
    subprocess.run([
        "ssh-keygen", "-q", "-t", "rsa", "-b", "3072", "-N", "", "-C",
        "temporary-hadoop-lab", "-f", str(private_path),
    ], check=True)
    private_path.chmod(0o600)
    public_path.chmod(0o644)
    key_name = f"{config['key_pair_name_prefix']}-{stamp}"
    return key_name, str(private_path), public_path.read_text(encoding="utf-8").strip(), str(public_path)


def call_model(client: Any, model_module: Any, method_name: str, request_name: str,
               request_data: dict[str, Any]):
    """按名称创建 SDK 请求并调用对应的 ECS API"""
    request_type = getattr(model_module, request_name)
    return getattr(client, method_name)(request_type(**request_data))


def auto_release_at(now: datetime, minutes: int) -> datetime:
    """计算自动释放时间，并向上取整到整分钟"""
    target = now + timedelta(minutes=minutes)
    if target.second or target.microsecond:
        target = target.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return target


def wait_instances(client: Any, models: Any, region: str, ids: list[str], timeout: int):
    """轮询实例状态，直到全部运行或出现失败、超时"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = call_model(client, models, "describe_instances", "DescribeInstancesRequest", {
            "region_id": region,
            "instance_ids": json.dumps(ids),
        })
        instances = [item.to_map() for item in response.body.instances.instance]
        states = {item.get("InstanceId"): item.get("Status") for item in instances}
        print(f"实例就绪进度：{sum(s == 'Running' for s in states.values())}/{len(ids)}")
        if len(instances) == len(ids) and all(s == "Running" for s in states.values()):
            return instances
        if any(s in {"CreateFailed", "Deleted", "Deleting"} for s in states.values()):
            raise RuntimeError(f"实例创建失败或已删除：{states}")
        time.sleep(8)
    raise TimeoutError("等待实例运行超时；实例仍会按创建请求中的自动释放时间到期释放")


def ssh_command(host: str, user: str, private_key: str, command: str,
                timeout: int = 20) -> subprocess.CompletedProcess[str]:
    """使用指定私钥执行一次 SSH 命令"""
    return subprocess.run([
        "ssh", "-i", private_key,
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=7",
        "-o", f"UserKnownHostsFile={private_key}.known_hosts",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ServerAliveInterval=8",
        f"{user}@{host}", command,
    ], text=True, capture_output=True, timeout=timeout, check=False)


def ssh_endpoint(node: dict[str, Any]) -> str:
    """优先返回公网地址，没有公网地址时使用内网地址"""
    return node.get("public_ip") or node["private_ip"]


def wait_ssh(nodes: list[dict[str, Any]], private_key: str, user: str, timeout: int) -> None:
    """并行等待所有实例的 SSH 服务就绪"""
    pending = {node["instance_id"]: node for node in nodes}
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        with ThreadPoolExecutor(max_workers=32) as pool:
            jobs = {pool.submit(ssh_command, ssh_endpoint(n), user, private_key, "true", 12): iid
                    for iid, n in pending.items()}
            for future in as_completed(jobs):
                iid = jobs[future]
                if future.result().returncode == 0:
                    del pending[iid]
        print(f"SSH 就绪进度：{len(nodes) - len(pending)}/{len(nodes)}")
        if pending:
            time.sleep(6)
    if pending:
        raise TimeoutError(f"以下实例 SSH 不可达：{list(pending)}")


def configure_hosts(nodes: list[dict[str, Any]], private_key: str, user: str) -> None:
    """配置节点内网名称解析，并把私钥分发到各节点以建立互信"""
    entries = "\n".join(f"{n['private_ip']} {n['name']}" for n in nodes)
    command = "printf '%s\\n' " + shlex.quote(entries + " # hadoop-lab-managed") + " >> /etc/hosts"
    with ThreadPoolExecutor(max_workers=24) as pool:
        list(pool.map(lambda n: subprocess.run([
            "ssh", "-i", private_key, "-o", "BatchMode=yes",
            "-o", f"UserKnownHostsFile={private_key}.known_hosts",
            "-o", "StrictHostKeyChecking=accept-new",
            f"{user}@{ssh_endpoint(n)}", command,
        ], check=True, timeout=25), nodes))

    key_data = Path(private_key).read_text(encoding="utf-8")
    key_command = "install -d -m 700 ~/.ssh && cat > ~/.ssh/id_rsa && chmod 600 ~/.ssh/id_rsa"
    with ThreadPoolExecutor(max_workers=24) as pool:
        list(pool.map(lambda node: subprocess.run([
            "ssh", "-i", private_key, "-o", "BatchMode=yes",
            "-o", f"UserKnownHostsFile={private_key}.known_hosts",
            "-o", "StrictHostKeyChecking=accept-new",
            f"{user}@{ssh_endpoint(node)}", key_command,
        ], input=key_data, text=True, check=True, timeout=25), nodes))


def bootstrap_cluster(nodes: list[dict[str, Any]], private_key: str, user: str) -> None:
    """向主节点和子节点运行 Hadoop 初始化脚本"""
    master = nodes[0]
    workers = nodes[1:]
    master_host = ssh_endpoint(master)
    private_master = master["name"]
    with tempfile.TemporaryDirectory() as temporary:
        workers_file = Path(temporary) / "workers"
        workers_file.write_text("".join(f"{node['name']}\n" for node in workers), encoding="utf-8")
        subprocess.run(["scp", "-i", private_key, "-o", f"UserKnownHostsFile={private_key}.known_hosts",
                        "-o", "StrictHostKeyChecking=accept-new", str(workers_file),
                        f"{user}@{master_host}:/tmp/hadoop-workers"], check=True)
        script_url = "https://raw.githubusercontent.com/YY404NF/hadoop-deploy-and-test/main/bootstrap-hadoop-node.sh"
        for role, node, args in [
            ("master", master, f"{private_master} /tmp/hadoop-workers"),
            *( ("worker", node, f"{private_master} /dev/null") for node in workers ),
        ]:
            command = (
                f"set -o pipefail && curl -fsSL {script_url} "
                f"| RUN_USER=hadoop bash -s -- {role} {args}"
            )
            subprocess.run([
                "ssh", "-i", private_key, "-o", f"UserKnownHostsFile={private_key}.known_hosts",
                "-o", "StrictHostKeyChecking=accept-new", f"{user}@{ssh_endpoint(node)}", command,
            ], check=True)


def write_state(path: Path, state: dict[str, Any]) -> None:
    """以受限权限原子写入部署状态文件"""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


if __name__ == "__main__":
    # 读取部署参数，并确定状态文件和 ECS API 地域。
    config = load_config()
    state_path = BASE_DIR / config["output_file"]
    client = make_client(config["region_id"])
    # 导入 ECS SDK 的请求模型。
    from alibabacloud_ecs20140526 import models

    # 生成本地 SSH 密钥，并将公钥导入阿里云。
    key_name, private_key, public_key, public_key_path = make_keypair(config)
    call_model(client, models, "import_key_pair", "ImportKeyPairRequest", {
            "region_id": config["region_id"],
            "key_pair_name": key_name,
            "public_key_body": public_key,
    })
    # 计算创建时间和实例的服务端自动释放时间。
    request_time = datetime.now(timezone.utc)
    release_time = auto_release_at(request_time, int(config["lifetime_minutes"]))
    # 组装实例规格、网络、安全组、密钥和系统盘等创建参数。
    request_data = {
            "region_id": config["region_id"],
            "image_id": config["image_id"],
            "instance_type": config["instance_type"],
            "v_switch_id": config["v_switch_id"],
            "security_group_id": config["security_group_id"],
            "internet_max_bandwidth_out": 1,
            "key_pair_name": key_name,
            "amount": int(config["instance_count"]),
            "min_amount": int(config["instance_count"]),
            "auto_release_time": release_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "system_disk": models.RunInstancesRequestSystemDisk(
                size=str(config["system_disk_size_gb"]),
            ),
            "description": "Temporary Hadoop course cluster",
    }
    # 显示即将提交的参数，并发送批量创建请求。
    print("创建计划：")
    print(json.dumps({**request_data, "vpc_id": config["vpc_id"]}, ensure_ascii=False, indent=2,
                     default=lambda value: value.to_map() if hasattr(value, "to_map") else str(value)))
    print("准备创建实例；凭证和 SSH 私钥不会输出。")
    response = call_model(client, models, "run_instances", "RunInstancesRequest", request_data)
    ids = response.body.instance_id_sets.instance_id_set or []
    print(f"已创建 {len(ids)} 台，预计 {release_time.isoformat()} 自动释放。")
    # 等待实例全部启动；失败或超时直接报错。
    instances = wait_instances(client, models, config["region_id"], ids, 900)
    # 防止实例启动耗时超过自动释放时间后继续部署。
    if datetime.now(timezone.utc) >= release_time:
        raise TimeoutError("实例启动已超过自动释放时间；请在阿里云控制台确认实例状态")

    # 提取每台实例的地址和 ID，生成集群脚本共用的节点列表。
    nodes = []
    for index, instance in enumerate(instances, start=1):
        private_ip = instance["VpcAttributes"]["PrivateIpAddress"]["IpAddress"][0]
        public_ip = instance["PublicIpAddress"]["IpAddress"][0]
        nodes.append({
            "name": f"{config['name_prefix']}-{index:03d}",
            "instance_id": instance["InstanceId"],
            "private_ip": private_ip,
            "public_ip": public_ip,
            "ssh_user": config["ssh_user"],
        })
    # 等待公网 SSH 可用，并设置内网主机名解析和节点间密钥互信。
    print("等待 SSH 可连接……")
    wait_ssh(nodes, private_key, config["ssh_user"], 600)
    print("配置内网主机名映射……")
    configure_hosts(nodes, private_key, config["ssh_user"])
    # 再检查一次剩余时长，避免在到期后启动初始化流程。
    if datetime.now(timezone.utc) >= release_time:
        raise TimeoutError("部署已超过自动释放时间；请在阿里云控制台确认实例状态")

    # 汇总集群、密钥和实例信息，供测试程序读取。
    state = {
            "region_id": config["region_id"],
            "instance_type": config["instance_type"],
            "instance_count": len(nodes),
            "vpc_id": config["vpc_id"],
            "v_switch_id": config["v_switch_id"],
            "zone_id": instances[0]["ZoneId"],
            "security_group_id": config["security_group_id"],
            "created_at": request_time.isoformat().replace("+00:00", "Z"),
            "expires_at": release_time.isoformat().replace("+00:00", "Z"),
            "ssh_user": config["ssh_user"],
            "ssh_private_key_file": private_key,
            "ssh_public_key_file": public_key_path,
            "key_pair_name": key_name,
            "hadoop_home": config.get("hadoop_home", "/usr/local/hadoop"),
            "nodes": nodes,
    }
    # 写出状态文件并显示连接密钥位置和自动释放时间。
    write_state(state_path, state)
    print(f"部署完成，状态文件：{state_path}")
    print(f"SSH 私钥：{private_key}")
    print(f"预计自动释放时间：{state['expires_at']}")
    print("安装并启动 Hadoop 集群……")
    # 初始化 Hadoop 主从节点，然后运行独立测试程序。
    bootstrap_cluster(nodes, private_key, config["ssh_user"])
    print("运行 HDFS 与 MapReduce 测试……")
    raise SystemExit(subprocess.run([sys.executable, str(BASE_DIR / "test.py")], check=False).returncode)
