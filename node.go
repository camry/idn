package idn

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/google/uuid"
    "github.com/spf13/cast"
    clientv3 "go.etcd.io/etcd/client/v3"
)

type Node struct {
    opts   *nodeOptions       // 节点选项
    ctx    context.Context    // 节点上下文
    cancel context.CancelFunc // 节点取消函数
}

// NewNode 创建节点
func NewNode(opts ...NodeOption) (*Node, error) {
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
        return nil, gerror.Newf("NewNode Id %s NodeName is empty", o.id)
    }
    if o.etcdClient == nil {
        return nil, gerror.Newf("NewNode Id %s NodeName %s etcd client is nil", o.id, o.name)
    }
    ctx, cancel := context.WithCancel(o.ctx)
    return &Node{
        opts:   o,
        ctx:    ctx,
        cancel: cancel,
    }, nil
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
    // 注销节点
    nodeServiceKey := fmt.Sprintf(ServiceKey, x.opts.name, x.opts.id)
    _, err := x.opts.etcdClient.Delete(x.ctx, nodeServiceKey)
    if err != nil {
        return gerror.Wrapf(err, "Node Stop Id %s NodeName %s ServiceKey %s Delete etcd failed", x.opts.id, x.opts.name, nodeServiceKey)
    }
    // 删除ID映射
    discoveryMappingsPrefixKey := fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.name)
    resp, err := x.opts.etcdClient.Get(x.ctx, discoveryMappingsPrefixKey, clientv3.WithPrefix())
    if err != nil {
        return gerror.Wrapf(err, "Node Stop Id %s NodeName %s DiscoveryMappingsPrefix %s Get etcd failed", x.opts.id, x.opts.name, discoveryMappingsPrefixKey)
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
    return nil
}
