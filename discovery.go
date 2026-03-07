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
)

type Discovery[T IDType] struct {
    opts   *discoveryOptions  // 发现服务选项
    ctx    context.Context    // 发现服务上下文
    cancel context.CancelFunc // 发现服务取消函数
    ncMap  sync.Map           // 节点配置映射
    icMap  sync.Map           // ID 配置映射
}

// NewDiscovery 创建发现服务
func NewDiscovery[T IDType](opts ...DiscoveryOption) *Discovery[T] {
    o := &discoveryOptions{
        ctx: context.Background(),
    }
    for _, opt := range opts {
        opt(o)
    }
    ctx, cancel := context.WithCancel(o.ctx)
    return &Discovery[T]{
        ctx:    ctx,
        cancel: cancel,
        opts:   o,
    }
}

// GRPCClient 获取 ID 对应的 gRPC 客户端连接
func (x *Discovery[T]) GRPCClient(id T) (*grpc.ClientConn, error) {
    v, ok := x.icMap.Load(id)
    if ok {
        if ic, ok1 := v.(*idConfig); ok1 {
            return ic.grpcClient, nil
        }
        return nil, gerror.Newf("Discovery NodeName %s ID %v GRPCClient id config not found", x.opts.nodeName, id)
    }
    // TODO 从节点字典中获取负载最小的节点，并初始化 ID 配置，更新对应节点的负载值
    return nil, nil
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
