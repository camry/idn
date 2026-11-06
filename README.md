# idn

基于 etcd + Redis 的分布式节点注册、发现与 gRPC 负载均衡库。

## 功能特性

- **节点注册**：通过 etcd 租约机制将节点元信息（ID、地址、版本等）注册到集群，租约到期自动下线
- **ID 绑定**：将业务 ID（如用户 UID）绑定到指定节点，通过 Redis 存储映射关系并设置 TTL
- **负载追踪**：在 Redis Hash 中维护每个节点的实时负载计数，绑定/解绑时原子更新
- **服务发现**：启动时从 etcd 拉取全量节点，并通过 Watch 实时感知节点上下线
- **gRPC 连接管理**：自动为每个节点维护 gRPC 长连接，节点变更时自动替换，使用 singleflight 防止重复创建
- **最小负载路由**：`GrpcClient` 优先路由到 ID 绑定的节点，未找到时回退到负载最小的节点
- **类型安全**：通过 Go 泛型支持 `int32 / int64 / uint32 / uint64 / string` 等多种 ID 类型

## 安装

```bash
go get github.com/camry/idn
```

**依赖要求**：etcd v3、Redis

## 架构概览

```
Node (服务端)                        Discovery (客户端)
  │                                      │
  ├─ etcd 租约注册                        ├─ etcd 拉取全量节点
  │   /{project}/idn/nodes/{name}/        │   Watch 实时更新
  │     service/{id} → JSON              │
  │                                      ├─ Redis 查询 ID→节点映射
  ├─ Redis 负载哈希                        │   {project}:idn:{name}:{idName}:{id}
  │   {project}:idn:{name}:load           │
  │   field=nodeID, value=loadCount      ├─ Redis 查询最小负载节点
  │                                      │   {project}:idn:{name}:load
  └─ Redis ID 映射                        │
      {project}:idn:{name}:{idName}:{id} └─ 返回 gRPC 连接
        → nodeID (带 TTL)
```

## 快速开始

### 节点服务（服务端）

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/g/v2/gnet/gipv4"
    "github.com/redis/go-redis/v9"
    clientv3 "go.etcd.io/etcd/client/v3"

    "github.com/camry/idn"
)

func main() {
    etcdClient, _ := clientv3.New(clientv3.Config{
        Endpoints:   []string{"127.0.0.1:2379"},
        DialTimeout: 5 * time.Second,
    })
    redisClient := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379",
    })

    intranetIp, _ := gipv4.GetIntranetIp()

    ns, err := idn.NewNode[int32](
        idn.NodeProject("myapp"),           // 项目名称，默认 "wild"
        idn.NodeName("game-server"),        // 节点类型名称，默认 "uid"
        idn.NodeVersion("1.0.0"),
        idn.NodeGRPCAddr(fmt.Sprintf("%s:6001", intranetIp)),
        idn.NodeEtcdClient(etcdClient),     // 必填
        idn.NodeRedisClient(redisClient),   // 必填
        idn.NodeLeaseTTL(30),               // 租约 TTL（秒），默认 30
    )
    if err != nil {
        panic(err)
    }

    app := dove.New(
        dove.Name("game-server"),
        dove.Context(context.Background()),
        dove.Server(ns),
    )

    // 节点启动后绑定业务 ID
    time.AfterFunc(time.Second, func() {
        // 将用户 UID=1001 绑定到本节点，映射有效期 5 分钟
        _ = ns.BindID("uid", int32(1001), 5*time.Minute)

        // 刷新映射 TTL（客户端心跳场景）
        _ = ns.HeartbeatID("uid", int32(1001), 5*time.Minute)

        // 检查 ID 是否已绑定
        ok, _ := ns.HasID("uid", int32(1001))
        fmt.Println("has uid 1001:", ok)

        // 解绑 ID（用户下线）
        _ = ns.UnbindID("uid", int32(1001))
    })

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

### 服务发现（客户端）

```go
package main

import (
    "context"
    "time"

    "github.com/camry/dove/v2"
    "github.com/redis/go-redis/v9"
    clientv3 "go.etcd.io/etcd/client/v3"

    "github.com/camry/idn"
)

func main() {
    etcdClient, _ := clientv3.New(clientv3.Config{
        Endpoints:   []string{"127.0.0.1:2379"},
        DialTimeout: 5 * time.Second,
    })
    redisClient := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379",
    })

    ds, err := idn.NewDiscovery[int32](
        idn.DiscoveryProject("myapp"),          // 与 Node 保持一致
        idn.DiscoveryNodeName("game-server"),   // 与 Node 保持一致
        idn.DiscoveryEtcdClient(etcdClient),    // 必填
        idn.DiscoveryRedisClient(redisClient),  // 必填
        idn.DiscoveryMinLoadNodeTTL(time.Minute), // 最小负载节点缓存时长，默认 1 分钟
    )
    if err != nil {
        panic(err)
    }

    app := dove.New(
        dove.Name("gateway"),
        dove.Context(context.Background()),
        dove.Server(ds),
    )

    time.AfterFunc(time.Second, func() {
        // 获取 UID=1001 所在节点的 gRPC 连接；未找到则路由到负载最小节点
        conn, err := ds.GrpcClient("uid", int32(1001))
        if err != nil {
            panic(err)
        }
        _ = conn // 用 conn 构造业务 gRPC stub

        // 仅查询 ID 绑定的节点元信息
        if nc, ok := ds.IDNode("uid", int32(1001)); ok {
            fmt.Println("node addr:", nc.GetGRPCAddr())
        }

        // 直接获取当前负载最小的节点
        if nc, ok := ds.MinLoadNode(); ok {
            fmt.Println("min load node:", nc.GetID())
        }
    })

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

## API 参考

### Node[T IDType]

| 方法                             | 说明                                     |
|--------------------------------|----------------------------------------|
| `NewNode[T](...NodeOption)`    | 创建节点，id 默认为随机 UUID                     |
| `Start(ctx)`                   | 向 etcd 注册节点（带租约），初始化 Redis 负载计数，启动租约续期 |
| `Stop(ctx)`                    | 撤销 etcd 租约，删除 Redis 负载条目               |
| `BindID(idName, id, ttl)`      | Redis 事务：写入 ID→nodeID 映射 + 负载计数 +1     |
| `UnbindID(idName, id)`         | Redis 事务：删除 ID 映射 + 负载计数 -1            |
| `HeartbeatID(idName, id, ttl)` | 刷新 ID 映射的 TTL                          |
| `HasID(idName, id)`            | 检查 ID 映射是否存在                           |
| `ID()`                         | 返回节点 ID                                |
| `Name()`                       | 返回节点名称                                 |
| `Version()`                    | 返回节点版本                                 |
| `Addr()`                       | 返回节点 gRPC 地址                           |

#### NodeOption

| 选项函数                   | 默认值                    | 说明                 |
|------------------------|------------------------|--------------------|
| `NodeID(id)`           | 随机 UUID                | 节点唯一 ID            |
| `NodeProject(project)` | `"wild"`               | 项目名称，用于 key 命名空间隔离 |
| `NodeName(name)`       | `"uid"`                | 节点类型名称             |
| `NodeVersion(version)` | `""`                   | 版本号                |
| `NodeBuildTime(t)`     | `""`                   | 构建时间               |
| `NodeBuildCommit(c)`   | `""`                   | Git Commit         |
| `NodeHTTPAddr(addr)`   | `""`                   | HTTP 监听地址          |
| `NodeGRPCAddr(addr)`   | `""`                   | gRPC 监听地址          |
| `NodeTCPAddr(addr)`    | `""`                   | TCP 监听地址           |
| `NodeCtx(ctx)`         | `context.Background()` | 节点根 Context        |
| `NodeEtcdClient(c)`    | **必填**                 | etcd 客户端           |
| `NodeRedisClient(c)`   | **必填**                 | Redis 客户端          |
| `NodeLeaseTTL(ttl)`    | `30`                   | etcd 租约 TTL（秒）     |

### Discovery[T IDType]

| 方法                                    | 说明                                                |
|---------------------------------------|---------------------------------------------------|
| `NewDiscovery[T](...DiscoveryOption)` | 创建发现服务                                            |
| `Start(ctx)`                          | 从 etcd 拉取全量节点并建立 gRPC 连接，启动 Watch 协程              |
| `Stop(ctx)`                           | 关闭所有 gRPC 连接，清空节点映射                               |
| `GrpcClient(idName, id)`              | 获取 gRPC 连接：优先 ID 绑定节点，回退最小负载节点；singleflight 防重复创建 |
| `IDNode(idName, id)`                  | 查询 ID 绑定的节点元信息，返回 `INodeConfig`                   |
| `MinLoadNode()`                       | 返回当前负载最小的节点（有缓存）                                  |

#### DiscoveryOption

| 选项函数                                           | 默认值                    | 说明                |
|------------------------------------------------|------------------------|-------------------|
| `DiscoveryProject(project)`                    | `"wild"`               | 项目名称，须与 Node 一致   |
| `DiscoveryNodeName(name)`                      | `"uid"`                | 节点类型名称，须与 Node 一致 |
| `DiscoveryCtx(ctx)`                            | `context.Background()` | 根 Context         |
| `DiscoveryEtcdClient(c)`                       | **必填**                 | etcd 客户端          |
| `DiscoveryRedisClient(c)`                      | **必填**                 | Redis 客户端         |
| `DiscoveryGrpcDialOptions(...grpc.DialOption)` | insecure + keepalive   | 自定义 gRPC 拨号选项     |
| `DiscoveryMinLoadNodeTTL(ttl)`                 | `1m`                   | 最小负载节点 LRU 缓存 TTL |
