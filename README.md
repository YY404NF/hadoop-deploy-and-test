# 阿里云 ECS Hadoop 集群与矩阵计算测试

## 预配置

在当前目录中创建并按下列格式配置 `.aliyun-accessKey`：

```json
{
  "AccessKeyId": "替换为你的 AccessKey ID",
  "AccessKeySecret": "替换为你的 AccessKey Secret"
}
```

## 初始化环境并安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 创建 ECS Hadoop 集群、配置互信并初始化集群环境

1. 准备部署脚本配置文件 `deploy-config.jsonc`。

该配置文件包含创建 ECS 实例的规格、存储容量、创建数量、自动释放时间等字段。

2. 运行部署脚本

```bash
python3 deploy.py
```

部署脚本会先创建 ECS 实例并配置节点间 SSH 互信，然后在所有节点运行 `bootstrap-hadoop-node.sh`：

- 主节点：安装 Java 和 Hadoop、配置并启动 NameNode/ResourceManager

- 子节点：安装 Java 和 Hadoop、配置并启动 DataNode/NodeManager

部署脚本会在当前目录生成下列文件：

| 名称 | 作用 |
| --- | --- |
| `deployment-state.json` | 本次运行基本信息 |
| `hadoop-lab-id_rsa-{时间戳}` | SSH 私钥，用于登录 ECS 和节点间互信 |
| `hadoop-lab-id_rsa-{时间戳}.pub` | SSH 公钥，导入阿里云密钥对 |
| `hadoop-lab-id_rsa-{时间戳}.known_hosts` | 保存本次连接的实例主机指纹 |

## 运行矩阵乘法测试

1. 准备测试脚本配置文件 `test-matrix-config.jsonc`。

该配置文件包含测试矩阵规格、矩阵分块大小、重复测试次数、Reduce 任务数、切片大小和超时时间字段。

2. 运行测试脚本

```bash
python3 test-matrix.py
```

测试脚本会在 ECS Hadoop 集群上运行 `run-hadoopp-matrix.sh` 进行自动测试。

测试脚本会在当前目录生成下列文件：

| 名称 | 作用 |
| --- | --- |
| `matrix-results-{时刻}` | 需运算的矩阵及相关文件 |
| `result.json` | 计算结果耗时 |
