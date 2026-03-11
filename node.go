package idn

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/google/uuid"
    "github.com/spf13/cast"
    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
)

type Node struct {
    opts   *nodeOptions
    ctx    context.Context
    cancel context.CancelFunc
}

// NewNode 创建节点
func NewNode(opts ...NodeOption) *Node {
    o := &nodeOptions{
        ctx: context.Background(),
    }
    if id, err := uuid.NewUUID(); err == nil {
        o.id = id.String()
    }
    for _, opt := range opts {
        opt(o)
    }
    ctx, cancel := context.WithCancel(o.ctx)
    return &Node{
        opts:   o,
        ctx:    ctx,
        cancel: cancel,
    }
}

// ID 返回节点ID
func (x *Node) ID() string {
    return x.opts.id
}

// Name 返回节点名称
func (x *Node) Name() string {
    return x.opts.name
}

// Version 返回节点版本
func (x *Node) Version() string {
    return x.opts.version
}

// Addr 返回节点地址
func (x *Node) Addr() string {
    return x.opts.addr
}

// Start 启动节点
func (x *Node) Start(_ context.Context) error {
    if x.opts.etcdClient == nil {
        return gerror.Newf("Node Id %s NodeName %s Start etcd client is nil", x.opts.id, x.opts.name)
    }
    session, err := concurrency.NewSession(x.opts.etcdClient, concurrency.WithTTL(5))
    if err != nil {
        return gerror.Wrapf(err, "Node Id %s NodeName %s Start new etcd session error", x.opts.id, x.opts.name)
    }
    defer session.Close()
    mu := concurrency.NewMutex(session, fmt.Sprintf(MutexServicePrefix, x.opts.name))
    if err = mu.Lock(x.ctx); err != nil {
        return gerror.Wrapf(err, "Node Id %s NodeName %s Start lock etcd mutex error", x.opts.id, x.opts.name)
    }
    defer mu.Unlock(x.ctx)
    // 注册节点
    nc := &nodeConfig{
        ID:      x.opts.id,
        Name:    x.opts.name,
        Version: x.opts.version,
        Addr:    x.opts.addr,
    }
    v, err := json.Marshal(nc)
    if err != nil {
        return gerror.Wrapf(err, "Node Id %s NodeName %s Start marshal node config failed", x.opts.id, x.opts.name)
    }
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(ServiceKey, x.opts.name, x.opts.id), string(v))
    if err != nil {
        return gerror.Wrapf(err, "Node Id %s NodeName %s Start register to etcd failed", x.opts.id, x.opts.name)
    }
    return nil
}

// Stop 停止节点
func (x *Node) Stop(_ context.Context) error {
    session, err := concurrency.NewSession(x.opts.etcdClient, concurrency.WithTTL(5))
    if err != nil {
        return gerror.Wrapf(err, "Node Id %s NodeName %s Stop new etcd session error", x.opts.id, x.opts.name)
    }
    defer session.Close()
    mu := concurrency.NewMutex(session, fmt.Sprintf(MutexServicePrefix, x.opts.name))
    if err = mu.Lock(x.ctx); err != nil {
        return gerror.Wrapf(err, "Node Id %s NodeName %s Stop lock etcd mutex error", x.opts.id, x.opts.name)
    }
    defer mu.Unlock(x.ctx)
    // 注销节点
    nodeServiceKey := fmt.Sprintf(ServiceKey, x.opts.name, x.opts.id)
    _, err = x.opts.etcdClient.Delete(x.ctx, nodeServiceKey)
    if err != nil {
        return gerror.Wrapf(err, "Node Stop Id %s NodeName %s ServiceKey %s Delete etcd failed", x.opts.id, x.opts.name, nodeServiceKey)
    }
    // 删除ID映射
    resp, err := x.opts.etcdClient.Get(x.ctx, fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.name), clientv3.WithPrefix())
    if err != nil {
        return gerror.Wrapf(err, "Node Stop Id %s NodeName %s DiscoveryMappingsPrefix %s Get etcd failed", x.opts.id, x.opts.name, DiscoveryMappingsPrefix)
    }
    for _, kv := range resp.Kvs {
        nodeId := cast.ToString(kv.Value)
        if nodeId == x.opts.id {
            _, err = x.opts.etcdClient.Delete(x.ctx, string(kv.Key))
            if err != nil {
                return gerror.Wrapf(err, "Node Stop Id %s NodeName %s DiscoveryMappingsPrefix %s Delete etcd failed", x.opts.id, x.opts.name, kv.Key)
            }
        }
    }
    if x.cancel != nil {
        x.cancel()
    }
    return nil
}
