package sdn

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/google/uuid"
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
        ctx:    ctx,
        cancel: cancel,
        opts:   o,
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
        return gerror.Newf("Node Id %s Name %s Start etcd client is nil", x.opts.id, x.opts.name)
    }
    // 注册节点
    nodeConfig := NodeConfig{
        ID:      x.opts.id,
        Name:    x.opts.name,
        Version: x.opts.version,
        Addr:    x.opts.addr,
    }
    v, err := json.Marshal(nodeConfig)
    if err != nil {
        return gerror.Wrapf(err, "Node Id %s Name %s Start marshal node config failed", x.opts.id, x.opts.name)
    }
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf("/nodes/%s/service/%s", x.opts.name, x.opts.id), string(v))
    if err != nil {
        return gerror.Wrapf(err, "Node Id %s Name %s Start register to etcd failed", x.opts.id, x.opts.name)
    }
    return nil
}

// Stop 停止节点
func (x *Node) Stop(_ context.Context) error {
    // 注销节点
    _, err := x.opts.etcdClient.Delete(x.ctx, fmt.Sprintf("/nodes/%s/service/%s", x.opts.name, x.opts.id))
    if err != nil {
        return gerror.Wrapf(err, "Node Id %s Name %s Stop unregister from etcd failed", x.opts.id, x.opts.name)
    }
    if x.cancel != nil {
        x.cancel()
    }
    return nil
}
