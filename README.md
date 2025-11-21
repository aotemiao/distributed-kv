# distributed-kv

一个基于 HashiCorp Raft + Consul 实现的分布式 Key-Value 存储系统。项目特性：
- **Raft 共识算法**：使用 Raft 实现分布式一致性
- **Consul 服务发现**：自动服务注册、健康检查、节点发现
- **自动集群引导**：使用分布式锁确保集群安全引导，支持动态节点加入
- **HTTP API**：提供简单的 REST API，非 Leader 节点自动代理请求
- **持久化存储**：使用 BoltDB 存储 Raft 日志和状态

适合用于学习 Raft、Consul 集成、分布式系统设计。


## 目录结构

```
.
├─ cmd/server              # 可执行程序入口
├─ internal/discovery      # Consul 服务发现与注册
├─ internal/node           # Raft 节点创建与引导
├─ internal/store          # 状态机（FSM）实现
├─ internal/transport      # HTTP 服务与请求代理
└─ data/                   # Raft 数据目录（运行时生成）
```


## 架构概览

- **服务发现**：使用 Consul 进行节点注册、健康检查和服务发现
- **集群引导**：
  - 前 3 个健康节点使用 Consul 分布式锁协调，确保只引导一次
  - 后续节点通过 `/join` API 自动加入现有集群
- **一致性保障**：使用 Raft 协议保证数据一致性
- **数据存储**：
  - FSM：内存 map[string]string，通过 Raft 日志同步
  - 持久化：BoltDB（日志、稳定存储）+ 文件快照
- **请求路由**：Follower 节点自动将写请求代理到 Leader


## 依赖环境

- Go 1.21+
- Consul（用于服务发现）
- Windows / macOS / Linux

### 安装 Consul

**Windows (PowerShell):**
```powershell
# 下载并解压 Consul
# https://www.consul.io/downloads
# 将 consul.exe 放到系统 PATH 中
```

**macOS:**
```bash
brew install consul
```

**Linux:**
```bash
wget https://releases.hashicorp.com/consul/1.17.0/consul_1.17.0_linux_amd64.zip
unzip consul_1.17.0_linux_amd64.zip
sudo mv consul /usr/local/bin/
```


## 快速开始

### 1. 启动 Consul

在单独的终端窗口启动 Consul 开发模式：

```bash
consul agent -dev
```

Consul 将监听在 `127.0.0.1:8500`，可通过 http://127.0.0.1:8500/ui 访问管理界面。

### 2. 构建项目

```bash
go mod download
go build -o dist-kv.exe ./cmd/server    # Windows
# 或
go build -o dist-kv ./cmd/server        # macOS/Linux
```

### 3. 启动集群节点

**启动前 3 个节点**（在不同的终端窗口）：

```bash
# 节点 1
./dist-kv

# 节点 2
./dist-kv

# 节点 3
./dist-kv
```

前 3 个节点会自动协调并引导集群。查看日志输出确认集群形成：
```
2025/10/16 16:00:00 HTTP server started at 127.0.0.1:xxxxx
2025/10/16 16:00:00 Registering service 'node-xxxxxxxx' with Consul at ...
2025/10/16 16:00:02 Found 3 peers. Attempting to acquire bootstrap lock
2025/10/16 16:00:02 Acquired bootstrap lock. Will bootstrap cluster
2025/10/16 16:00:02 Raft node injected into HTTP server. System is fully operational.
```

**启动第 4 个及更多节点**：

```bash
./dist-kv
```

新节点会自动发现现有集群并请求加入：
```
2025/10/16 16:01:00 Found 3 other healthy peers. Joining existing cluster
2025/10/16 16:01:00 新节点将等待被现有集群添加...
2025/10/16 16:01:02 Successfully joined cluster via 127.0.0.1:xxxxx
```


## 命令行参数

```bash
./dist-kv [flags]
```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-http` | HTTP 服务地址 | 自动选择随机端口 |
| `-id` | 节点 ID | 自动生成（node-xxxxxxxx）|
| `-raft` | Raft 通信地址 | 自动选择随机端口 |
| `-dir` | 数据存储目录 | `data` |
| `-consul` | Consul 服务地址 | `127.0.0.1:8500` |

**示例：指定固定端口**

```bash
./dist-kv -id node1 -http 127.0.0.1:8001 -raft 127.0.0.1:9001
./dist-kv -id node2 -http 127.0.0.1:8002 -raft 127.0.0.1:9002
./dist-kv -id node3 -http 127.0.0.1:8003 -raft 127.0.0.1:9003
```


## HTTP API

### 1. 写入/更新键值

```bash
# 请求
POST http://<HTTP_ADDR>/kv
Content-Type: application/json

{
  "key": "foo",
  "value": "bar"
}

# PowerShell 示例
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8001/kv `
  -Body '{"key":"foo","value":"bar"}' `
  -ContentType 'application/json'

# curl 示例
curl -X POST http://127.0.0.1:8001/kv \
  -H "Content-Type: application/json" \
  -d '{"key":"foo","value":"bar"}'
```

### 2. 读取键值

```bash
# 请求
GET http://<HTTP_ADDR>/kv?key=foo

# PowerShell 示例
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8001/kv?key=foo"

# curl 示例
curl http://127.0.0.1:8001/kv?key=foo

# 响应
{"key":"foo","value":"bar"}
```

### 3. 删除键值

```bash
# 请求
DELETE http://<HTTP_ADDR>/kv?key=foo

# PowerShell 示例
Invoke-RestMethod -Method Delete -Uri "http://127.0.0.1:8001/kv?key=foo"

# curl 示例
curl -X DELETE http://127.0.0.1:8001/kv?key=foo
```

### 4. 健康检查

```bash
GET http://<HTTP_ADDR>/health

# 返回 200 OK 表示节点健康
```

### 5. 节点加入（内部 API）

```bash
POST http://<LEADER_ADDR>/join
Content-Type: application/json

{
  "node_id": "node-xxx",
  "raft_addr": "127.0.0.1:9004"
}
```

此 API 由节点自动调用，通常不需要手动使用。


## 工作原理

### 集群引导流程

1. **节点启动**：
   - 启动 HTTP 服务器
   - 向 Consul 注册服务（包含健康检查）

2. **检查集群状态**：
   - 查询 Consul KV 中的 `distributed-kv/bootstrapped` 标记
   - 如果已引导，跳到步骤 4

3. **协调引导**（前 3 个节点）：
   - 等待至少 3 个健康节点
   - 尝试获取 Consul 分布式锁 `distributed-kv/bootstrap-lock`
   - 获取锁的节点执行集群引导
   - 设置引导标记，释放锁

4. **加入集群**（第 4+ 个节点）：
   - 创建 Raft 实例（不引导）
   - 通过 Consul 发现现有节点
   - 向任意节点发送 `/join` 请求
   - 请求被代理到 Leader，Leader 调用 `AddVoter()` 添加节点

### 请求路由

- **读请求**：可以发送到任意节点，直接从本地 FSM 读取
- **写请求**：必须由 Leader 处理
  - 发送到 Leader：直接处理
  - 发送到 Follower：自动代理到 Leader（通过 Consul 发现 Leader 的 HTTP 地址）


## 数据持久化

每个节点的数据存储在 `data/<node_id>/` 目录：

```
data/
└─ node-xxxxxxxx/
   ├─ raft-log.bolt       # Raft 日志
   ├─ raft-stable.bolt    # 稳定存储（term、vote等）
   └─ snapshots/          # 快照文件
```

- **日志存储**：记录所有状态变更操作
- **快照**：定期压缩日志，加速恢复
- **状态机**：内存 KV 存储，从日志/快照恢复


## 故障恢复与测试

### 测试节点故障

```bash
# 停止一个节点（Ctrl+C）
# 集群仍可正常工作（3节点可容忍1个故障）

# 重启节点
./dist-kv -id <same-node-id>

# 节点会自动重新加入集群并同步数据
```

### 测试 Leader 故障

```bash
# 找到当前 Leader（查看日志或通过 Consul UI）
# 停止 Leader 节点

# 剩余节点会自动选举新 Leader（通常 1-2 秒内完成）
# 写请求会短暂失败，然后恢复正常
```

### 清空数据重启

```bash
# 停止所有节点
# 删除数据目录
rm -rf data/

# 删除 Consul 中的引导标记
consul kv delete distributed-kv/bootstrapped

# 重新启动节点（会重新引导集群）
```


## 监控与调试

### 查看 Consul 服务

访问 Consul UI：http://127.0.0.1:8500/ui

- 查看注册的服务和健康状态
- 检查 KV 存储中的引导标记
- 监控节点注册/注销

### 查看 Raft 状态

节点日志会显示：
- Leader 选举信息
- 节点加入/离开
- 日志复制状态
- 快照创建


## 常见问题

**Q: 为什么需要 Consul？**
A: Consul 提供服务发现、健康检查、分布式锁，简化节点管理和集群引导。

**Q: 可以不用 Consul 吗？**
A: 可以，但需要手动配置节点列表，类似早期的 config.json 方式。

**Q: 最少需要几个节点？**
A: 3 个节点可容忍 1 个故障，5 个节点可容忍 2 个故障。单节点也能运行但无高可用。

**Q: 节点可以动态伸缩吗？**
A: 可以。新节点自动加入，移除节点需要调用 Raft 的 RemoveServer API（需额外实现）。

**Q: 数据丢失怎么办？**
A: 只要多数节点存活，数据就不会丢失。单节点故障不影响数据安全。

**Q: 性能如何？**
A: 这是一个学习项目，性能未优化。生产环境请使用 etcd、Consul KV 等成熟方案。


## 开发与测试

```bash
# 格式化代码
go fmt ./...

# 静态检查
go vet ./...

# 运行测试
go test ./...

# 构建
go build -o dist-kv ./cmd/server
```


## 技术栈

- [HashiCorp Raft](https://github.com/hashicorp/raft) - Raft 共识算法实现
- [HashiCorp Consul](https://www.consul.io/) - 服务发现与配置
- [raft-boltdb](https://github.com/hashicorp/raft-boltdb) - BoltDB 存储后端
- [BoltDB](https://github.com/etcd-io/bbolt) - 嵌入式 KV 数据库


## 许可与致谢

本项目仅作学习与示例用途，构建基于 HashiCorp 开源项目，感谢相关贡献者。
