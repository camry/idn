# idn — 分布式节点注册与路由库参考

调用此命令后，你已载入 `idn` 库的完整上下文。直接处理用户请求，无需再读文件做初步探索。

---

## 一、定位

`idn` 是一个 Go 泛型库，解决**有状态服务的请求路由问题**：

- 客户端 A（uid=10001）已连接到 server-3，下一条请求必须也到 server-3
- 新连接找不到绑定时，路由到当前负载最小的节点

依赖：**etcd**（节点注册/存活）+ **Redis**（ID 映射 + 负载计数）。

---

## 二、架构

```
┌─────────────────────────────────────────────────────┐
│  etcd                                               │
│  /project/idn/nodes/server/service/{nodeID}         │
│     └─ JSON: nodeConfig (addr, version, ...)        │
│     └─ 绑定 Lease，节点崩溃自动删除                    │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  Redis                                              │
│  {project}:idn:{name}:load          Hash            │
│     field=nodeID, value=负载计数                     │
│                                                     │
│  {project}:idn:{name}:{idName}:{id} String (TTL)    │
│     value=nodeID                                    │
└─────────────────────────────────────────────────────┘
```

### 两个主体

| 类型 | 职责 |
|---|---|
| `Node[T]` | 运行在服务节点。向 etcd 注册自身，维护 etcd Lease 续期，管理 ID→nodeID 映射的生命周期 |
| `Discovery[T]` | 运行在客户端/网关。Watch etcd 节点变化，维护本地 gRPC 连接池，路由请求 |

泛型参数 `T IDType` = `int32 | int64 | uint32 | uint64 | string`，表示业务 ID 的类型。

---

## 三、Redis 键格式（define.go）

```go
ServiceLoadKey  = "%s:idn:%s:load"            // Hash: project:idn:name:load
IDMappingKey    = "%s:idn:%s:%s:%v"           // String: project:idn:name:idName:id
MinLoadNodeCacheKey = "min-load-node"         // Discovery 本地内存缓存 key（非 Redis）
```

etcd 键：
```go
ServiceKey       = "/%s/idn/nodes/%s/service/%s"   // 单节点
ServiceKeyPrefix = "/%s/idn/nodes/%s/service"      // Watch 前缀
```

---

## 四、Node API（node.go）

### 生命周期
```go
// 实现 dove/server.Server 接口
func (x *Node[T]) Start(_ context.Context) error
func (x *Node[T]) Stop(_ context.Context) error
```

`Start` 做：
1. `etcdClient.Grant(leaseTTL)` — 申请租约（默认 30s）
2. `etcdClient.KeepAlive(leaseID)` — 启动自动续期
3. `etcdClient.Put(ServiceKey, nodeConfigJSON, WithLease)` — 注册节点
4. `redisClient.HSet(ServiceLoadKey, nodeID, 0)` — 初始化负载计数
5. 启动两个 goroutine：`watchLeaseKeepAlive`（消费续期响应）、`batchOpWorker`（批聚合写入）

`Stop` 做：
1. `isStopped.Store(true)` — 阻止新的 ID 操作入队
2. `etcdClient.Revoke(leaseID)` — 立即撤销租约
3. `redisClient.HDel(ServiceLoadKey, nodeID)` — 清理负载计数
4. `cancel()` — 触发 `batchOpWorker` 排空剩余队列后退出

### ID 管理（非阻塞，fire-and-forget）
```go
// BindID: 入队 batchOpBind，worker 批量执行 SET IDMappingKey + HIncrBy load +N
func (x *Node[T]) BindID(idName string, id T, ttl time.Duration) error

// UnbindID: 入队 batchOpUnbind，worker 批量执行 DEL IDMappingKey + HIncrBy load -N
func (x *Node[T]) UnbindID(idName string, id T) error

// HeartbeatID: 入队 batchOpHeartbeat，worker 批量执行 EXPIRE IDMappingKey ttl
func (x *Node[T]) HeartbeatID(idName string, id T, ttl time.Duration) error

// HasID: EXISTS IDMappingKey（直接调用，同步返回）
func (x *Node[T]) HasID(idName string, id T) (bool, error)
```

**关键行为**：`BindID`/`UnbindID`/`HeartbeatID` 均为非阻塞调用——将操作写入内部 channel 后立即返回，Redis 执行由 `batchOpWorker` 异步完成。仅在节点已 Stop 时返回错误，Redis 错误由 worker 内部 `glog.Error` 记录。

### 内部批聚合机制（node.go）

```go
type batchOpType uint8
const (
    batchOpBind      batchOpType = iota  // BindID
    batchOpUnbind                        // UnbindID
    batchOpHeartbeat                     // HeartbeatID
)

type batchOp[T IDType] struct {
    opType batchOpType
    idName string
    id     T
    ttl    time.Duration
}
```

`batchOpWorker` 逻辑（对标 `CollectorWorker.Run()`）：
- `case op, ok := <-batchCh`：接收操作，达到 `batchOpSize` 立即刷新
- `case <-ticker.C`：按 `batchOpFlushInterval` 定时刷新
- `case <-ctx.Done()`：`close(batchCh)` → `for range` 排空剩余 → 最后一批写入 → 退出

`doBatchOpWrite` 将一批操作合为一次 `TxPipelined`（MULTI/EXEC）：
- `batchOpBind` → `SET key nodeID ttl`，`incr++`
- `batchOpUnbind` → `DEL key`，`incr--`
- `batchOpHeartbeat` → `EXPIRE key ttl`（不影响 incr）
- `HIncrBy loadKey nodeID incr`（incr≠0 时追加）
- 全程使用 `context.Background()`，不受节点 ctx 取消影响

---

## 五、Discovery API（discovery.go）

### 生命周期
```go
func (x *Discovery[T]) Start(_ context.Context) error   // 加载全量节点 + Watch
func (x *Discovery[T]) Stop(_ context.Context) error    // 关闭所有 gRPC 连接
```

`Start` 做：
1. `etcdClient.Get(ServiceKeyPrefix, WithPrefix)` — 全量加载现有节点到 `ncMap` + `nGrpcMap`
2. goroutine `watchServiceNode(revision+1)` — 从当前 revision 起 Watch 增量变更

### 内部状态
```go
ncMap    sync.Map  // nodeID -> *nodeConfig
nGrpcMap sync.Map  // nodeID -> *grpc.ClientConn
minLoadNodeCache gcache.Cache  // LRU，容量50，key=MinLoadNodeCacheKey
sf singleflight.Group          // 防止同一 nodeID 并发建连
```

### 路由逻辑
```go
// GrpcClient：先查 ID 绑定，找不到走最小负载
func (x *Discovery[T]) GrpcClient(idName string, id T) (*grpc.ClientConn, error)

// IDNode：从 Redis 查 IDMappingKey → nodeID → ncMap
func (x *Discovery[T]) IDNode(idName string, id T) (INodeConfig, bool)

// MinLoadNode：HGetAll ServiceLoadKey，跳过 ncMap 中不存在的节点（已下线）
func (x *Discovery[T]) MinLoadNode() (INodeConfig, bool)
```

`minLoadNode` 有一级内存缓存（默认 TTL=1min，可通过 `DiscoveryMinLoadNodeTTL` 配置）。节点下线时若缓存命中该节点则立即 invalidate。

### Watch 事件处理（watchServiceNode）
- `PUT`：更新 `ncMap`，重建 gRPC 连接（先关旧连接）
- `DELETE`：从 `ncMap`/`nGrpcMap` 删除，关闭连接，若缓存的 minLoadNode 是该节点则清缓存

---

## 六、nodeConfig（node_config.go）

```go
type nodeConfig struct {
    ID          string
    Name        string
    Version     string
    BuildTime   string
    BuildCommit string
    HTTPAddr    string
    GRPCAddr    string
    TCPAddr     string
}
```

实现 `INodeConfig` 接口（define.go），供 Discovery 消费时类型安全访问。

---

## 七、Option 模式

### NodeOption（node_option.go）
| 函数 | 字段 | 默认值 |
|---|---|---|
| `NodeID` | id | uuid.NewUUID() |
| `NodeProject` | project | "wild" |
| `NodeName` | name | "uid" |
| `NodeLeaseTTL` | leaseTTL | 30 (秒) |
| `NodeEtcdClient` | etcdClient | **必填** |
| `NodeRedisClient` | redisClient | **必填** |
| `NodeGRPCAddr/HTTPAddr/TCPAddr` | 地址 | "" |
| `NodeBatchOpFlushInterval` | batchOpFlushInterval | 5ms |
| `NodeBatchOpSize` | batchOpSize | 500 |
| `NodeBatchOpChanSize` | batchOpChanSize | 10000 |

### DiscoveryOption（discovery_option.go）
| 函数 | 字段 | 默认值 |
|---|---|---|
| `DiscoveryProject` | project | "wild" |
| `DiscoveryNodeName` | nodeName | "uid" |
| `DiscoveryMinLoadNodeTTL` | minLoadNodeTTL | 1min |
| `DiscoveryGrpcDialOptions` | grpcDialOptions | insecure + keepalive(15s/3s/withoutStream) |
| `DiscoveryEtcdClient` | etcdClient | **必填** |
| `DiscoveryRedisClient` | redisClient | **必填** |

---

## 八、关键设计决策

1. **etcd 负责存活，Redis 负责路由**：etcd Lease 保证节点崩溃时自动清理，Redis 负责低延迟的 ID→节点查找和负载感知。

2. **Node.isStopped atomic.Bool**：`BindID`/`UnbindID`/`HeartbeatID` 在 Stop 后快速失败，防止继续向 batchCh 入队。

3. **batchOpWorker 写合并**：ID 操作不直接访问 Redis，统一入队 batchCh，由单一 worker 按时间窗口（5ms）或批大小（500）触发一次 `TxPipelined`，将 N 次 RTT 压缩为 1 次。游戏服务重启时 10,000 并发 BindID/UnbindID 约 1–2 个 RTT 完成，vs 改前 ~10,000 RTT。

4. **TxPipelined 而非 Pipelined**：批内所有 SET/DEL/EXPIRE + 最终 HIncrBy 包裹在 MULTI/EXEC 中原子提交，避免部分成功导致 load 计数与映射键不一致。回调内只入队命令，不检查 `.Err()`（入队阶段命令未执行，`.Err()` 恒为 nil）。

5. **doBatchOpWrite 使用 context.Background()**：worker 的 ctx.Done 分支在 ctx 已取消后仍需排空队列写入 Redis，使用 `context.Background()` 防止 ctx 取消导致最后一批数据丢失。

6. **Discovery.singleflight**：`GrpcClient` 中同一 nodeID 的首次建连只发生一次，并发请求等待同一结果。

7. **Watch 从 `revision+1` 开始**：`Start` 拉全量时记录 Header.Revision，Watch 从此 revision+1 起，保证全量与增量无缝衔接，不丢事件。

8. **minLoadNode 缓存失效策略**：只在节点下线事件（EventTypeDelete）时精确 invalidate，BindID/UnbindID 改变负载后不主动失效——允许短暂路由不均衡，换取低开销。

---

## 九、性能特征

| 操作 | Redis 命令 | 改前 RTT | 改后 RTT |
|---|---|---|---|
| BindID × N（并发） | SET + HIncrBy（批） | N 次 | **1 次**（5ms 窗口内合批） |
| UnbindID × N（并发） | DEL + HIncrBy（批） | N 次 | **1 次** |
| HeartbeatID × N（并发） | EXPIRE（批） | N 次（8,000/s @ 80k×10s） | **~200/s**（5ms 窗口 → 200批/s） |
| GrpcClient（IDNode 路径） | GET | 1 次 | 1 次（未变） |
| minLoadNode（缓存未命中） | HGetAll | 1 次 | 1 次（未变） |

重启场景（10,000 用户）：`batchOpChanSize=10000`，10,000 条操作在一个 5ms 窗口内全部入队，`doBatchOpWrite` 一次 TxPipelined 完成，端到端约 **0.5ms**，vs 改前串行约 **50ms**。

---

## 十、测试说明（*_z_test.go）

- 测试依赖真实 etcd（192.168.11.77:2379）和 Redis（192.168.11.77:6379），无 mock。
- `TestNewNode`：5 节点并发注册，1s 后绑定 500 个 ID，30s 后解绑，35s 后停止。
- `TestNewDiscovery`：5 个 Discovery 实例，并发 100 路 GrpcClient 查询。
- 测试文件名后缀 `_z_` 是项目约定，表示集成测试。