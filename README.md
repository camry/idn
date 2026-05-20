# idn

ID 分布式节点组件，提供基于 etcd 的服务注册、发现和负载均衡功能。

## 功能特性

- **节点管理**：创建、启动、停止节点，支持节点生命周期管理
- **服务注册与发现**：基于 etcd 的服务注册和发现机制
- **负载均衡**：根据节点负载自动分配请求
- **gRPC 客户端管理**：自动维护节点的 gRPC 连接
- **实时监听**：监听节点变化和 ID 映射变化
- **类型安全**：支持泛型 ID 类型

## 安装

```bash
go get gitea.meigushi.go/meigushi/idn
```

## 快速开始

### 创建节点服务

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    "github.com/camry/g/v2/glog"
    "github.com/camry/g/v2/gnet/gipv4"
    clientv3 "go.etcd.io/etcd/client/v3"

    "gitea.meigushi.go/meigushi/idn"
)

func main() {
    // 创建 etcd 客户端
    etcdClient, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"192.168.110.5:2379"},
        Username:    "root",
        Password:    "123456",
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        panic(err)
    }

    // 获取内网 IP
    intranetIp, err := gipv4.GetIntranetIp()
    if err != nil {
        panic(err)
    }

    // 创建节点服务
    ns, err := idn.NewNode[int32](
        idn.NodeName("guild"),
        idn.NodeVersion("1.0.0"),
        idn.NodeGRPCAddr(fmt.Sprintf("%s:%d", intranetIp, 6001)),
        idn.NodeEtcdClient(etcdClient),
    )
    if err != nil {
        panic(err)
    }

    ns.SetCallRemoveIDFunc(func(id int32) error {
        glog.Debugf("ns remove id %d", id)
        return nil
    })

    // 创建并运行 dove 应用
    app := dove.New(
        dove.Name("dove"),
        dove.Context(context.Background()),
        dove.Server(ns),
    )

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

### 创建发现服务

```go
package main

import (
    "context"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    clientv3 "go.etcd.io/etcd/client/v3"

    "gitea.meigushi.go/meigushi/idn"
)

func main() {
    // 创建 etcd 客户端
    etcdClient, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"192.168.110.5:2379"},
        Username:    "root",
        Password:    "123456",
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        panic(err)
    }

    // 创建发现服务
    ds,err := idn.NewDiscovery[int32](
        idn.DiscoveryNodeName("guild"),
        idn.DiscoveryEtcdClient(etcdClient),
        idn.DiscoveryLeaseTTL(5),
    )
    if err != nil {
        panic(err)
    }

    // 创建并运行 dove 应用
    app := dove.New(
        dove.Name("dove"),
        dove.Context(context.Background()),
        dove.Server(ds),
    )

    // 示例：获取 gRPC 客户端
    time.AfterFunc(time.Second*1, func() {
        client, err := ds.GrpcClient(int32(1))
        if err != nil {
            panic(err)
        }
        // 使用 client 进行 gRPC 调用
    })

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```
