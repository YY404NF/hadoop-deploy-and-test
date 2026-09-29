# 临时阿里云 ECS Hadoop 集群

`deploy.py` 创建 ECS、导入并使用本地 SSH 密钥、配置节点内网名称互通，最后写出 `deployment-state.json`。`bootstrap-hadoop-node.sh` 安装 JDK 8 和 Hadoop，并按 master/worker 角色配置和启动服务；`test.py` 读取状态文件检查 SSH/HDFS 并提交 Hadoop 自带 WordCount 作业。

## 配置

先编辑 `deploy-config.jsonc`。这是带中文注释的 JSONC 文件，已设置华南 2（河源）、`ecs.e-c1m1.large`、100 台和 31 分钟有效期，并填写了可复用的 VPC、交换机和安全组 ID。

网络资源需预先创建并填入 JSONC。部署程序直接将交换机和安全组 ID 传给 ECS；ID 无效或容量不足时，阿里云 API 报错后程序停止，不会自动发现或创建网络资源。

创建前请确认河源地域实例规格库存、账号配额、交换机内网地址容量和费用。运行脚本的电脑需要能 SSH 到实例；安全组需允许 TCP/22，并允许集群节点互访。`ssh_user` 应匹配系统镜像的登录账户；本作业使用的阿里云 Ubuntu 26.04 镜像默认使用 `root`。

部署代码会读取同目录下的 `.aliyun-accessKey` JSON 文件；包含 AccessKey ID 和 Secret 即可：

```json
{
  "AccessKeyId": "替换为你的 AccessKey ID",
  "AccessKeySecret": "替换为你的 AccessKey Secret"
}
```

不要把真实凭证贴进报告、提交到 Git 或发给他人。`.gitignore` 已忽略凭证、私钥及运行状态文件。

## 安装和运行

```bash
cd 代码
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

先检查 `deploy-config.jsonc` 中的地域、规格、实例数量、网络 ID 和有效时间。运行脚本会直接创建配置数量的实例：

```bash
python3 deploy.py
```

部署会在代码目录生成 SSH 密钥和权限为 `0600` 的 `deployment-state.json`。状态文件包含实例 ID、内外网 IP、SSH 用户、私钥路径、VPC/交换机/安全组、可用区和自动释放时间。

部署程序会自动安装、启动 Hadoop 并运行 `test.py`。测试包括主节点到所有节点的解析/SSH 检查、HDFS 报告，以及 Hadoop 示例 JAR 的 WordCount 作业和结果校验。

## 自动释放

ECS 创建请求本身携带服务端 `AutoReleaseTime`，按请求时间设置为 31 分钟后；时间以 UTC 表示并按分钟向后取整。到期由阿里云释放实例，不依赖本机后台进程运行。测试脚本会读取状态文件中的到期时间，过期后拒绝继续提交作业。

若部署中途失败，已成功创建的实例仍按创建请求中的服务端自动释放时间到期释放。实验完成后仍应检查密钥对等非计费资源。真实创建前请确认配置和云账号权限。

## Hadoop 初始化脚本

`deploy.py` 会让每台实例直接从 GitHub 下载 `bootstrap-hadoop-node.sh` 并执行。脚本通过阿里云 Ubuntu 软件源安装 OpenJDK 8；主节点从 Apache 镜像下载 Hadoop 3.4.2，再通过内网 HTTP 服务分发给子节点。Hadoop 不在该 Ubuntu 软件源中。最后自动运行 `test.py`。

单独手动初始化节点时，在主节点准备 workers 清单（一行一个内网主机名），然后运行正式脚本：

```bash
sudo RUN_USER=hadoop bash bootstrap-hadoop-node.sh master hadoop-lab-001 /tmp/hadoop-workers
```

在每个子节点运行：

```bash
sudo RUN_USER=hadoop bash bootstrap-hadoop-node.sh worker hadoop-lab-001
```

手动方式下，主节点从 Apache 镜像下载 Hadoop，子节点从主节点取包。所有节点需能访问 Ubuntu 软件源，且 master/worker 名称可通过内网解析。

`test-bootstrap-hadoop.sh` 是破坏性测试脚本：会安装软件、格式化测试用 NameNode 数据目录并启动服务。只在可丢弃的 Ubuntu 虚拟机/容器中以 root 运行，不要在已有 Hadoop 数据的机器上运行：

```bash
sudo bash test-bootstrap-hadoop.sh
```
