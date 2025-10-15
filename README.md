# distributed-kv

一个基于 HashiCorp Raft 实现的一致性分布式 Key-Value 存储示例。项目包含：
- Raft 状态机（FSM）实现：支持 SET / DELETE 操作，GET 读取本地内存状态
- HTTP 接口：对外提供 /kv API；非 Leader 节点会自动将写请求代理到 Leader
- 配置驱动的集群引导：通过 config.json 配置所有节点，无需额外服务发现
- BoltDB 日志与稳定存储，以及文件快照

适合用于学习 Raft、构建最小可运行的分布式一致性存储原型。


## 目录结构

```
.
├─ cmd/server            # 可执行程序入口
├─ internal/config       # 配置文件解析
├─ internal/node         # Raft 节点创建与引导
├─ internal/store        # 状态机（FSM）实现
├─ internal/transport    # HTTP 服务与代理
├─ config.json           # 示例集群配置（本机3节点）
└─ data/                 # Raft 数据目录（运行时生成）
```


## 架构概览

- 使用 HashiCorp Raft 实现副本状态机复制
- 每个节点内维护：
  - FSM：内存 map[string]string，Apply 日志实现 SET/DELETE；GET 走本地内存读
  - Raft 存储：BoltDB（日志、稳定存储）+ 文件快照
  - HTTP 服务：/kv 路径，Leader 处理写入；Follower 自动代理到 Leader
- 集群节点及地址从 config.json 统一加载；首次启动新节点时自动 Bootstrap 成为一个集群


## 依赖环境

- Go 1.21+（推荐与 go.mod 保持一致）
- Windows / macOS / Linux 均可运行（示例命令以 Windows PowerShell 为主）


## 快速开始（本机三节点示例）

1. 获取依赖并构建

```
# 在项目根目录执行
go mod download
go build -o dist-kv.exe ./cmd/server
```

2. 准备配置

项目已内置 config.json：

```
{
  "peers": [
    {"node_id": "node1", "raft_addr": "127.0.0.1:9001", "http_addr": "127.0.0.1:8001"},
    {"node_id": "node2", "raft_addr": "127.0.0.1:9002", "http_addr": "127.0.0.1:8002"},
    {"node_id": "node3", "raft_addr": "127.0.0.1:9003", "http_addr": "127.0.0.1:8003"}
  ]
}
```

3. 启动三个节点（分别在三个 PowerShell 窗口中）

```
# 窗口A
./dist-kv.exe -config config.json -id node1 -dir data

# 窗口B
./dist-kv.exe -config config.json -id node2 -dir data

# 窗口C
./dist-kv.exe -config config.json -id node3 -dir data
```

注意：
- 第一次启动会在 data/nodeX 目录下创建 raft-log.bolt、raft-stable.bolt 以及快照目录。
- Raft 会自动进行 leader 选举。非 Leader 节点会把写请求代理到 Leader。
- 关闭后再次启动会从已有存储中恢复，不会重复引导。


## HTTP API

- 基础路径：/kv
- Content-Type：application/json（POST 写入）

1) 写入/更新

- Method: POST
- URL: http://<HTTP_ADDR>/kv
- Body:
```
{
  "key": "foo",
  "value": "bar"
}
```
示例（PowerShell）：
```
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8001/kv -Body '{"key":"foo","value":"bar"}' -ContentType 'application/json'
```
说明：写请求只能由 Leader 处理；如果发送到 Follower，会被自动代理到 Leader。

2) 读取

- Method: GET
- URL: http://<HTTP_ADDR>/kv?key=foo

示例：
```
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8001/kv?key=foo"
```

响应：
```
{"key":"foo","value":"bar"}
```

3) 删除

- Method: DELETE
- URL: http://<HTTP_ADDR>/kv?key=foo

示例：
```
Invoke-RestMethod -Method Delete -Uri "http://127.0.0.1:8001/kv?key=foo"
```


## 运行参数

- -config：配置文件路径，默认 config.json
- -id：当前节点的 ID，必须与 config.json 中 peers[].node_id 匹配
- -dir：数据根目录，默认 data；每个节点的数据存放于 <dir>/<node_id>


## 配置说明（config.json）

- peers[].node_id：节点唯一 ID
- peers[].raft_addr：用于节点间 Raft 通讯的地址（TCP）
- peers[].http_addr：对外提供 HTTP API 的地址

HTTP 代理到 Leader 的逻辑会通过 peers 列表将 Leader 的 Raft 地址映射为其对应的 HTTP 地址。


## 数据与持久化

- 日志存储：data/<node_id>/raft-log.bolt
- 稳定存储：data/<node_id>/raft-stable.bolt
- 快照存储：data/<node_id>/（由 Raft 文件快照管理）

状态机（FSM）为内存 map；Raft 日志与快照保障在节点重启后的数据恢复。


## 常见问题与排错

- 启动顺序：任意顺序均可；首次启动时至少大多数节点可用才能完成选举。
- 端口被占用：修改 config.json 中的 http_addr / raft_addr 或停止占用的进程。
- 请求超时或 503：可能尚未选出 Leader（日志会提示），稍等或检查多数节点是否运行。
- Follower 收到写请求：会自动代理到 Leader，无需客户端感知。
- 清空数据重启：删除 data 目录后重新启动，将视为全新集群重新引导。


## 开发与测试

```
# 运行
go run ./cmd/server -config config.json -id node1 -dir data

# 代码风格与依赖
go fmt ./...
go vet ./...
```


## 许可与致谢

- 本项目仅作学习与示例用途。
- 构建基于 HashiCorp Raft 与 raft-boltdb，感谢相关开源项目。