package idn

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"
    "sync"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/google/uuid"
    "github.com/samber/lo"
    "github.com/spf13/cast"
    clientv3 "go.etcd.io/etcd/client/v3"
)

type Node[T IDType] struct {
    opts             *nodeOptions       // 节点选项
    ctx              context.Context    // 节点上下文
    cancel           context.CancelFunc // 节点取消函数
    callRemoveIDFunc func(id T) error   // 节点回调删除ID函数
}

// NewNode 创建节点
func NewNode[T IDType](opts ...NodeOption) (*Node[T], error) {
    o := &nodeOptions{
        ctx: context.Background(),
    }
    if id, err := uuid.NewUUID(); err == nil {
        o.id = id.String()
    }
    for _, opt := range opts {
        opt(o)
    }
    if o.name == "" {
        return nil, gerror.Newf("NewNode ID %s NodeName is empty", o.id)
    }
    if o.etcdClient == nil {
        return nil, gerror.Newf("NewNode ID %s NodeName %s etcd client is nil", o.id, o.name)
    }
    ctx, cancel := context.WithCancel(o.ctx)
    return &Node[T]{
        opts:   o,
        ctx:    ctx,
        cancel: cancel,
    }, nil
}

// ID 返回节点ID
func (x *Node[T]) ID() string {
    return x.opts.id
}

// Name 返回节点名称
func (x *Node[T]) Name() string {
    return x.opts.name
}

// Version 返回节点版本
func (x *Node[T]) Version() string {
    return x.opts.version
}

// Addr 返回节点地址
func (x *Node[T]) Addr() string {
    return x.opts.addr
}

// Start 启动节点
func (x *Node[T]) Start(_ context.Context) error {
    // 注册节点
    nc := &nodeConfig{
        ID:      x.opts.id,
        Name:    x.opts.name,
        Version: x.opts.version,
        Addr:    x.opts.addr,
    }
    v, err := json.Marshal(nc)
    if err != nil {
        return gerror.Wrapf(err, "Node ID %s NodeName %s Start marshal node config failed", x.opts.id, x.opts.name)
    }
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(ServiceKey, x.opts.name, x.opts.id), string(v))
    if err != nil {
        return gerror.Wrapf(err, "Node ID %s NodeName %s Start register to etcd failed", x.opts.id, x.opts.name)
    }
    var wg sync.WaitGroup
    wg.Go(x.watchIDMapping)
    wg.Wait()
    return nil
}

// Stop 停止节点
func (x *Node[T]) Stop(_ context.Context) error {
    defer x.cancel()
    // 注销节点
    nodeServiceKey := fmt.Sprintf(ServiceKey, x.opts.name, x.opts.id)
    _, err := x.opts.etcdClient.Delete(x.ctx, nodeServiceKey)
    if err != nil {
        return gerror.Wrapf(err, "Node Stop ID %s NodeName %s ServiceKey %s Delete etcd failed", x.opts.id, x.opts.name, nodeServiceKey)
    }
    // 删除ID映射
    discoveryMappingsPrefixKey := fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.name)
    resp, err := x.opts.etcdClient.Get(x.ctx, discoveryMappingsPrefixKey, clientv3.WithPrefix())
    if err != nil {
        return gerror.Wrapf(err, "Node Stop ID %s NodeName %s DiscoveryMappingsPrefix %s Get etcd failed", x.opts.id, x.opts.name, discoveryMappingsPrefixKey)
    }
    for _, kv := range resp.Kvs {
        nodeID := cast.ToString(kv.Value)
        if nodeID == x.opts.id {
            _, err = x.opts.etcdClient.Delete(x.ctx, string(kv.Key))
            if err != nil {
                return gerror.Wrapf(err, "Node Stop ID %s NodeName %s DiscoveryMappingsPrefix %s Delete etcd failed", x.opts.id, x.opts.name, kv.Key)
            }
        }
    }
    return nil
}

// BindID 绑定ID
func (x *Node[T]) BindID(id T) error {
    _, err := x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.name, id), x.opts.id)
    if err != nil {
        return gerror.Wrapf(err, "Node BindID ID %s NodeName %s put id %s etcd failed", x.opts.id, x.opts.name, id)
    }
    return nil
}

// UnbindID 解绑ID
func (x *Node[T]) UnbindID(id T) error {
    _, err := x.opts.etcdClient.Delete(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.name, id))
    if err != nil {
        return gerror.Wrapf(err, "Node UnbindID ID %s NodeName %s delete id %s etcd failed", x.opts.id, x.opts.name, id)
    }
    return nil
}

// SetCallRemoveIDFunc 设置节点回调删除ID函数
func (x *Node[T]) SetCallRemoveIDFunc(f func(id T) error) {
    x.callRemoveIDFunc = f
}

// watchIDMapping 监听 ID 映射
func (x *Node[T]) watchIDMapping() {
    watchChan := x.opts.etcdClient.Watch(x.ctx, fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.name), clientv3.WithPrefix(), clientv3.WithPrevKV())
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Node watchIDMapping NodeName %s NodeID %s Watch context canceled", x.opts.name, x.opts.id)
            return
        default:
            for response := range watchChan {
                for _, event := range response.Events {
                    switch event.Type {
                    case clientv3.EventTypeDelete:
                        glog.Debugf("Node watchIDMapping NodeName %s NodeID %s Key %s Value %s deleted", x.opts.name, x.opts.id, event.Kv.Key, event.PrevKv.Value)
                        nodeID := cast.ToString(event.PrevKv.Value)
                        if nodeID == x.opts.id {
                            // 处理键被删除的逻辑
                            idV, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                            if !ok {
                                glog.Error(gerror.Newf("Node watchIDMapping NodeName %s Key %s Last Split failed", x.opts.name, event.Kv.Key))
                                continue
                            }
                            id := cast.To[T](idV)
                            if x.callRemoveIDFunc != nil {
                                err := x.callRemoveIDFunc(id)
                                if err != nil {
                                    glog.Error(gerror.Wrapf(err, "Node watchIDMapping NodeName %s Key %s Delete failed", x.opts.name, event.Kv.Key))
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
