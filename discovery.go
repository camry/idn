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
    clientv3 "go.etcd.io/etcd/client/v3"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

type Discovery[T IDType] struct {
    opts     *discoveryOptions  // 发现服务选项
    ctx      context.Context    // 发现服务上下文
    cancel   context.CancelFunc // 发现服务取消函数
    ncMap    sync.Map           // 节点配置映射
    nGrpcMap sync.Map           // 节点 gRPC 客户端连接映射
    icMap    sync.Map           // ID 配置映射
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
func (x *Discovery[T]) GrpcClient(id T) (*grpc.ClientConn, error) {
    v, ok := x.icMap.Load(id)
    if ok {
        if ic, ok1 := v.(*idConfig); ok1 {
            return ic.grpcClient, nil
        }
        return nil, gerror.Newf("Discovery NodeName %s ID %v GrpcClient id config not found", x.opts.nodeName, id)
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
    // 创建 ID 配置
    ic := &idConfig{
        nodeConfig: minLoadNode,
        grpcClient: grpcClient,
    }
    // 存储 ID 配置
    x.icMap.Store(id, ic)
    // 更新对应节点的负载值
    minLoadNode.Load++
    // 将更新后的节点配置写回 etcd
    ncBytes, err := json.Marshal(minLoadNode)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery NodeName %s GrpcClient marshal node config error", x.opts.nodeName)
    }
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(NodeServiceKey, x.opts.nodeName, minLoadNode.ID), string(ncBytes))
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery NodeName %s GrpcClient put etcd error", x.opts.nodeName)
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
    return nil
}

// Stop 停止发现服务
func (x *Discovery[T]) Stop(ctx context.Context) error {
    if x.cancel != nil {
        x.cancel()
    }
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
                        nodeID, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchServiceNode NodeName %s Key %s Last Split error", x.opts.nodeName, event.Kv.Key))
                            continue
                        }
                        x.ncMap.Delete(nodeID)
                    }
                }
            }
        }
    }
}
