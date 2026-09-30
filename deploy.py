#!/usr/bin/env python3
"""创建短时 ECS 集群并配置节点间 SSH 互信"""

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
from zoneinfo import ZoneInfo


import json5


BASE_DIR = Path(__file__).resolve().parent
BEIJING = ZoneInfo("Asia/Shanghai")


def log(message: str) -> None:
    print(f"[{datetime.now(BEIJING):%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def progress(message: str) -> None:
    sys.stdout.write(f"\r\033[2K[{datetime.now(BEIJING):%Y-%m-%d %H:%M:%S}] {message}")
    sys.stdout.flush()


def finish_progress() -> None:
    print(flush=True)


def known_hosts_option(private_key: str) -> str:
    path = f"{private_key}.known_hosts".replace("\\", "\\\\").replace('"', '\\"')
    return f'UserKnownHostsFile="{path}"'


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
    ready_ids = set()
    progress(f"实例就绪进度：0/{len(ids)}")
    while time.monotonic() < deadline:
        response = call_model(client, models, "describe_instances", "DescribeInstancesRequest", {
            "region_id": region,
            "instance_ids": json.dumps(ids),
            "page_size": 100,
        })
        instances = [item.to_map() for item in response.body.instances.instance]
        states = {item.get("InstanceId"): item.get("Status") for item in instances}
        for instance_id in ids:
            if states.get(instance_id) == "Running" and instance_id not in ready_ids:
                ready_ids.add(instance_id)
                progress(f"实例就绪进度：{len(ready_ids)}/{len(ids)}")
        if len(instances) == len(ids) and all(s == "Running" for s in states.values()):
            finish_progress()
            return instances
        if any(s in {"CreateFailed", "Deleted", "Deleting"} for s in states.values()):
            finish_progress()
            raise RuntimeError(f"实例创建失败或已删除：{states}")
        time.sleep(8)
    finish_progress()
    raise TimeoutError("等待实例运行超时；实例仍会按创建请求中的自动释放时间到期释放")


def ssh_command(host: str, user: str, private_key: str, command: str,
                timeout: int = 20) -> subprocess.CompletedProcess[str]:
    """使用指定私钥执行一次 SSH 命令"""
    return subprocess.run([
        "ssh", "-i", private_key,
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=7",
        "-o", known_hosts_option(private_key),
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ServerAliveInterval=8",
        f"{user}@{host}", command,
    ], capture_output=True, timeout=timeout, check=False)


def ssh_endpoint(node: dict[str, Any]) -> str:
    """优先返回公网地址，没有公网地址时使用内网地址"""
    return node.get("public_ip") or node["private_ip"]


def wait_ssh(nodes: list[dict[str, Any]], private_key: str, user: str, timeout: int) -> None:
    """并行等待所有实例的 SSH 服务就绪"""
    pending = {node["instance_id"]: node for node in nodes}
    deadline = time.monotonic() + timeout
    ready = 0
    progress(f"SSH 就绪进度：0/{len(nodes)}")
    while pending and time.monotonic() < deadline:
        with ThreadPoolExecutor(max_workers=32) as pool:
            jobs = {pool.submit(ssh_command, ssh_endpoint(n), user, private_key, "true", 12): iid
                    for iid, n in pending.items()}
            for future in as_completed(jobs):
                iid = jobs[future]
                if future.result().returncode == 0:
                    del pending[iid]
                    ready += 1
                    progress(f"SSH 就绪进度：{ready}/{len(nodes)}")
        if pending:
            time.sleep(6)
    if pending:
        finish_progress()
        raise TimeoutError(f"以下实例 SSH 不可达：{list(pending)}")
    finish_progress()


def configure_hosts(nodes: list[dict[str, Any]], private_key: str, user: str) -> None:
    """配置节点内网名称解析，并把私钥分发到各节点以建立互信"""
    entries = "\n".join(f"{n['private_ip']} {n['name']}" for n in nodes)
    command = "printf '%s\\n' " + shlex.quote(entries + " # hadoop-lab-managed") + " >> /etc/hosts"
    with ThreadPoolExecutor(max_workers=100) as pool:
        list(pool.map(lambda n: subprocess.run([
            "ssh", "-i", private_key, "-o", "BatchMode=yes",
            "-o", known_hosts_option(private_key),
            "-o", "StrictHostKeyChecking=accept-new",
            f"{user}@{ssh_endpoint(n)}", command,
        ], check=True, timeout=25), nodes))

    key_data = Path(private_key).read_text(encoding="utf-8")
    key_command = "install -d -m 700 ~/.ssh && cat > ~/.ssh/id_rsa && chmod 600 ~/.ssh/id_rsa"
    with ThreadPoolExecutor(max_workers=100) as pool:
        list(pool.map(lambda node: subprocess.run([
            "ssh", "-i", private_key, "-o", "BatchMode=yes",
            "-o", known_hosts_option(private_key),
            "-o", "StrictHostKeyChecking=accept-new",
            f"{user}@{ssh_endpoint(node)}", key_command,
        ], input=key_data, text=True, check=True, timeout=25), nodes))


def bootstrap_cluster(nodes: list[dict[str, Any]], private_key: str, user: str) -> None:
    """上传本地初始化脚本，在主节点和子节点安装配置 Hadoop"""
    master = nodes[0]
    workers = nodes[1:]
    script = BASE_DIR / "bootstrap-hadoop-node.sh"
    with tempfile.TemporaryDirectory() as temporary:
        workers_file = Path(temporary) / "workers"
        workers_file.write_text("".join(f"{node['name']}\n" for node in workers), encoding="utf-8")
        subprocess.run([
            "scp", "-q", "-i", private_key,
            "-o", known_hosts_option(private_key),
            "-o", "StrictHostKeyChecking=accept-new",
            str(workers_file), f"{user}@{ssh_endpoint(master)}:/tmp/hadoop-workers",
        ], check=True)

        def upload_script(node: dict[str, Any]) -> None:
            subprocess.run([
                "scp", "-q", "-i", private_key,
                "-o", known_hosts_option(private_key),
                "-o", "StrictHostKeyChecking=accept-new",
                str(script), f"{user}@{ssh_endpoint(node)}:/tmp/bootstrap-hadoop-node.sh",
            ], check=True)

        def run_phase(role: str, node: dict[str, Any], args: str, phase: str) -> None:
            command = f"RUN_USER=hadoop bash /tmp/bootstrap-hadoop-node.sh {role} {args} {phase}"
            subprocess.run([
                "ssh", "-i", private_key,
                "-o", known_hosts_option(private_key),
                "-o", "StrictHostKeyChecking=accept-new",
                f"{user}@{ssh_endpoint(node)}", command,
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

        # 先并行上传全部节点脚本，再统一执行远端准备命令
        progress(f"节点脚本上传进度：0/{len(nodes)}")
        with ThreadPoolExecutor(max_workers=100) as pool:
            jobs = {pool.submit(upload_script, node): node for node in nodes}
            completed = 0
            for future in as_completed(jobs):
                future.result()
                completed += 1
                progress(f"节点脚本上传进度：{completed}/{len(nodes)}，{jobs[future]['name']}")
        finish_progress()

        def prepare_node(node: dict[str, Any]) -> None:
            role = "master" if node == master else "worker"
            args = f"{master['name']} /tmp/hadoop-workers" if role == "master" else f"{master['name']} /dev/null"
            run_phase(role, node, args, "prepare")

        progress(f"节点准备进度：0/{len(nodes)}")
        with ThreadPoolExecutor(max_workers=100) as pool:
            jobs = {pool.submit(prepare_node, node): node for node in nodes}
            completed = 0
            for future in as_completed(jobs):
                future.result()
                completed += 1
                node = jobs[future]
                progress(f"节点准备进度：{completed}/{len(nodes)}，{node['name']}")
        finish_progress()

        progress(f"初始化节点进度：0/{len(nodes)}")
        with ThreadPoolExecutor(max_workers=100) as pool:
            jobs = {}
            for node in nodes:
                role = "master" if node == master else "worker"
                args = f"{master['name']} /tmp/hadoop-workers" if role == "master" else f"{master['name']} /dev/null"
                jobs[pool.submit(run_phase, role, node, args, "configure")] = node
            completed = 0
            for future in as_completed(jobs):
                future.result()
                completed += 1
                node = jobs[future]
                progress(f"初始化节点进度：{completed}/{len(nodes)}，{node['name']}")
        finish_progress()


def write_state(path: Path, state: dict[str, Any]) -> None:
    """以受限权限原子写入部署状态文件"""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


if __name__ == "__main__":
    # 读取部署参数，并确定状态文件和 ECS API 地域
    config = load_config()
    state_path = BASE_DIR / config["output_file"]
    client = make_client(config["region_id"])
    # 导入 ECS SDK 的请求模型
    from alibabacloud_ecs20140526 import models

    # 生成本地 SSH 密钥，并将公钥导入阿里云
    key_name, private_key, public_key, public_key_path = make_keypair(config)
    call_model(client, models, "import_key_pair", "ImportKeyPairRequest", {
            "region_id": config["region_id"],
            "key_pair_name": key_name,
            "public_key_body": public_key,
    })
    # 计算创建时间和实例的服务端自动释放时间
    request_time = datetime.now(timezone.utc)
    release_time = auto_release_at(request_time, int(config["lifetime_minutes"]))
    # 组装实例规格、网络、安全组、密钥和系统盘等创建参数
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
    # 显示即将提交的参数，并发送批量创建请求
    log("创建计划：" + json.dumps(
        {**request_data, "vpc_id": config["vpc_id"]}, ensure_ascii=False, separators=(",", ":"),
        default=lambda value: value.to_map() if hasattr(value, "to_map") else str(value),
    ))
    log("准备创建实例")
    response = call_model(client, models, "run_instances", "RunInstancesRequest", request_data)
    ids = response.body.instance_id_sets.instance_id_set or []
    log(f"已创建 {len(ids)} 台，预计 {release_time.astimezone(BEIJING).isoformat()} 自动释放")
    # 等待实例全部启动；失败或超时直接报错
    instances = wait_instances(client, models, config["region_id"], ids, 900)
    # 防止实例启动耗时超过自动释放时间后继续部署
    if datetime.now(timezone.utc) >= release_time:
        raise TimeoutError("实例启动已超过自动释放时间；请在阿里云控制台确认实例状态")

    # 提取每台实例的地址和 ID，生成集群脚本共用的节点列表
    nodes = []
    for index, instance in enumerate(instances, start=1):
        private_ip = instance["VpcAttributes"]["PrivateIpAddress"]["IpAddress"][0]
        public_ip = instance["PublicIpAddress"]["IpAddress"][0]
        nodes.append({
            "name": f"{config['name_prefix']}-{index:03d}",
            "instance_id": instance["InstanceId"],
            "private_ip": private_ip,
            "public_ip": public_ip,
        })
    # 等待公网 SSH 可用，并设置内网主机名解析和节点间密钥互信
    log("等待 SSH 可连接……")
    wait_ssh(nodes, private_key, config["ssh_user"], 600)
    log("配置内网主机名映射……")
    configure_hosts(nodes, private_key, config["ssh_user"])
    # 再检查一次剩余时长，避免在到期后启动初始化流程
    if datetime.now(timezone.utc) >= release_time:
        raise TimeoutError("部署已超过自动释放时间；请在阿里云控制台确认实例状态")

    # 汇总集群、密钥和实例信息，供测试程序读取
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
    # 写出状态文件并显示连接密钥位置和自动释放时间
    write_state(state_path, state)
    log(f"部署完成，状态文件：{state_path}")
    log(f"SSH 私钥：{private_key}")
    log(f"预计自动释放时间：{release_time.astimezone(BEIJING).isoformat()}")
    log("初始化 Hadoop 主从节点……")
    bootstrap_cluster(nodes, private_key, config["ssh_user"])
    log("Hadoop 集群初始化完成")
