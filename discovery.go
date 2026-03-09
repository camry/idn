package sdn

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"
    "sync"
    "time"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/samber/lo"
    "github.com/spf13/cast"
    clientv3 "go.etcd.io/etcd/client/v3"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

type Discovery[T IDType] struct {
    opts     *discoveryOptions  // 发现服务选项
    ctx      context.Context    // 发现服务上下文
    cancel   context.CancelFunc // 发现服务取消函数
    ncMap    sync.Map           // 节点配置映射 （节点 ID -> 节点配置）
    nGrpcMap sync.Map           // 节点 gRPC 客户端连接映射 （节点 ID -> gRPC 客户端连接）
    icMap    sync.Map           // ID 配置映射 （ID -> 节点 ID）
}

// NewDiscovery 创建发现服务
func NewDiscovery[T IDType](opts ...DiscoveryOption) *Discovery[T] {
    o := &discoveryOptions{
        ctx: context.Background(),
    }
    for _, opt := range opts {
        opt(o)
    }
    // 设置默认 gRPC 客户端连接选项
    if len(o.grpcDialOptions) == 0 {
        o.grpcDialOptions = []grpc.DialOption{
            grpc.WithTransportCredentials(insecure.NewCredentials()),
        }
    }
    ctx, cancel := context.WithCancel(o.ctx)
    return &Discovery[T]{
        ctx:    ctx,
        cancel: cancel,
        opts:   o,
    }
}

// GrpcClient 获取 ID 对应的 gRPC 客户端连接
func (x *Discovery[T]) GrpcClient(id T, isNotSave ...bool) (*grpc.ClientConn, error) {
    nodeId, ok := x.icMap.Load(id)
    if ok {
        if grpcClient, ok2 := x.nGrpcMap.Load(nodeId); ok2 {
            return grpcClient.(*grpc.ClientConn), nil
        }
        return nil, gerror.Newf("Discovery NodeName %s ID %v GrpcClient node %s not found", x.opts.nodeName, id, nodeId)
    }
    // 从节点字典中获取负载最小的节点
    var minLoadNode *nodeConfig
    x.ncMap.Range(func(key, value interface{}) bool {
        if nc, ok1 := value.(*nodeConfig); ok1 {
            if minLoadNode == nil || nc.Load < minLoadNode.Load {
                minLoadNode = nc
            }
        }
        return true
    })
    if minLoadNode == nil {
        return nil, gerror.Newf("Discovery NodeName %s GrpcClient no available nodes", x.opts.nodeName)
    }
    // 创建 gRPC 客户端连接
    grpcClient, err := grpc.NewClient(minLoadNode.Addr, x.opts.grpcDialOptions...)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery NodeName %s GrpcClient dial error", x.opts.nodeName)
    }
    // 存储 ID 配置
    x.icMap.Store(id, minLoadNode.ID)
    // 存储节点 gRPC 客户端连接
    x.nGrpcMap.Store(minLoadNode.ID, grpcClient)
    if len(isNotSave) > 0 && isNotSave[0] {
        return grpcClient, nil
    }
    // 更新对应节点的负载值
    minLoadNode.Load++
    // TODO 存储当前 Discovery 节点负载值
    // 将更新后的 ID 映射配置写回 etcd
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(IdMappingKey, x.opts.nodeName, id), minLoadNode.ID)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery NodeName %s GrpcClient put node id %s etcd error", x.opts.nodeName, minLoadNode.ID)
    }
    // 将更新后的节点配置写回 etcd
    ncBytes, err := json.Marshal(minLoadNode)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery NodeName %s GrpcClient marshal node config error", x.opts.nodeName)
    }
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(NodeServiceKey, x.opts.nodeName, minLoadNode.ID), string(ncBytes))
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery NodeName %s GrpcClient put node id %s etcd error", x.opts.nodeName, minLoadNode.ID)
    }
    return grpcClient, nil
}

// Start 启动发现服务
func (x *Discovery[T]) Start(ctx context.Context) error {
    if x.opts.etcdClient == nil {
        return gerror.Newf("Discovery NodeName %s Start etcd client is nil", x.opts.nodeName)
    }
    // 获取所有服务节点
    resp, err := x.opts.etcdClient.Get(ctx, fmt.Sprintf(DiscoveryServiceKey, x.opts.nodeName), clientv3.WithPrefix())
    if err != nil {
        return gerror.Wrapf(err, "Discovery NodeName %s Get etcd error", x.opts.nodeName)
    }
    for _, kv := range resp.Kvs {
        nc := &nodeConfig{}
        err = json.Unmarshal(kv.Value, nc)
        if err != nil {
            return gerror.Wrapf(err, "Discovery NodeName %s Key %s Value %s Unmarshal etcd error", x.opts.nodeName, kv.Key, kv.Value)
        }
        glog.Debugf("Discovery NodeName %s Key %s Value %s", x.opts.nodeName, kv.Key, kv.Value)
        x.ncMap.Store(nc.ID, nc)
    }
    // 协程监听服务节点
    go x.watchServiceNode()
    // 协程监听 ID 映射
    go x.watchIDMapping()
    return nil
}

// Stop 停止发现服务
func (x *Discovery[T]) Stop(ctx context.Context) error {
    if x.cancel != nil {
        x.cancel()
    }
    // 从 etcd 清空 ID 配置映射
    x.icMap.Range(func(key, value interface{}) bool {
        _, err := x.opts.etcdClient.Delete(ctx, fmt.Sprintf(IdMappingKey, x.opts.nodeName, key))
        if err != nil {
            glog.Error(gerror.Wrapf(err, "Discovery Stop NodeName %s Delete id %v etcd error", x.opts.nodeName, key))
        }
        return true
    })
    // 关闭所有 gRPC 客户端连接
    x.nGrpcMap.Range(func(key, value interface{}) bool {
        if grpcClient, ok := value.(*grpc.ClientConn); ok {
            _ = grpcClient.Close()
        }
        return true
    })
    x.nGrpcMap.Clear()
    x.ncMap.Clear()
    x.icMap.Clear()
    return nil
}

// watchServiceNode 监听服务节点
func (x *Discovery[T]) watchServiceNode() {
    key := fmt.Sprintf(DiscoveryServiceKey, x.opts.nodeName)
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Discovery watchServiceNode NodeName %s Watch context canceled", x.opts.nodeName)
            return
        default:
            // 创建 watcher
            watchChan := x.opts.etcdClient.Watch(x.ctx, key, clientv3.WithPrefix())

            // 持续处理 watch 事件
            for response := range watchChan {
                if response.Err() != nil {
                    glog.Error(gerror.Wrapf(response.Err(), "Discovery watchServiceNode NodeName %s Watch etcd error", x.opts.nodeName))
                    // 短暂延迟后重试
                    time.Sleep(time.Second)
                    break // 跳出循环，重新创建 watcher
                }

                // 处理每个事件
                for _, event := range response.Events {
                    switch event.Type {
                    case clientv3.EventTypePut:
                        glog.Debugf("Discovery watchServiceNode NodeName %s Key %s updated: %s", x.opts.nodeName, event.Kv.Key, event.Kv.Value)
                        // 处理键被添加或更新的逻辑
                        nc := &nodeConfig{}
                        err := json.Unmarshal(event.Kv.Value, nc)
                        if err != nil {
                            glog.Error(gerror.Wrapf(err, "Discovery watchServiceNode NodeName %s Key %s Value %s Unmarshal etcd error", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                            continue
                        }
                        x.ncMap.Store(nc.ID, nc)
                    case clientv3.EventTypeDelete:
                        glog.Debugf("Discovery watchServiceNode NodeName %s Key %s deleted", x.opts.nodeName, event.Kv.Key)
                        // 处理键被删除的逻辑
                        nodeId, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchServiceNode NodeName %s Key %s Last Split error", x.opts.nodeName, event.Kv.Key))
                            continue
                        }
                        x.icMap.Range(func(key, value any) bool {
                            idK := cast.To[T](key)
                            nodeIdV := cast.ToString(value)
                            if nodeIdV == nodeId {
                                x.icMap.Delete(idK)
                            }
                            return true
                        })
                        if grpcClient, ok1 := x.nGrpcMap.Load(nodeId); ok1 {
                            if conn, ok2 := grpcClient.(*grpc.ClientConn); ok2 {
                                _ = conn.Close()
                            }
                            x.nGrpcMap.Delete(nodeId)
                        }
                        x.ncMap.Delete(nodeId)
                    }
                }
            }
        }
    }
}

// watchIDMapping 监听 ID 映射
func (x *Discovery[T]) watchIDMapping() {
    key := fmt.Sprintf(DiscoveryMappingsKey, x.opts.nodeName)
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Discovery watchIDMapping NodeName %s Watch context canceled", x.opts.nodeName)
            return
        default:
            // 创建 watcher
            watchChan := x.opts.etcdClient.Watch(x.ctx, key, clientv3.WithPrefix())

            // 持续处理 watch 事件
            for response := range watchChan {
                if response.Err() != nil {
                    glog.Error(gerror.Wrapf(response.Err(), "Discovery watchIDMapping NodeName %s Watch etcd error", x.opts.nodeName))
                    // 短暂延迟后重试
                    time.Sleep(time.Second)
                    break // 跳出循环，重新创建 watcher
                }

                // 处理每个事件
                for _, event := range response.Events {
                    switch event.Type {
                    case clientv3.EventTypePut:
                        glog.Debugf("Discovery watchIDMapping NodeName %s Key %s updated: %s", x.opts.nodeName, event.Kv.Key, event.Kv.Value)
                        // 处理键被添加或更新的逻辑
                        id := cast.To[T](event.Kv.Key)
                        nodeId := cast.ToString(event.Kv.Value)
                        _, ok := x.icMap.Load(id)
                        if !ok {
                            ncV, ok1 := x.ncMap.Load(nodeId)
                            if !ok1 {
                                glog.Error(gerror.Newf("Discovery watchIDMapping NodeName %s Key %s Value %s Load NodeConfig not found", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                                continue
                            }
                            nc, ok2 := ncV.(*nodeConfig)
                            if !ok2 {
                                glog.Error(gerror.Newf("Discovery watchIDMapping NodeName %s Key %s Value %s Match NodeConfig Failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                                continue
                            }
                            grpcClient, err := grpc.NewClient(nc.Addr, x.opts.grpcDialOptions...)
                            if err != nil {
                                glog.Error(gerror.Wrapf(err, "Discovery watchIDMapping NodeName %s Key %s Value %s GrpcClient dial error", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                                continue
                            }
                            x.icMap.Store(id, nodeId)
                            x.nGrpcMap.Store(nodeId, grpcClient)
                        }
                    }
                }
            }
        }
    }
}
