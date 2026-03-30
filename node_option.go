package idn

import (
    "context"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
)

type NodeOption func(o *nodeOptions)

type nodeOptions struct {
    id           string
    name         string
    version      string
    addr         string
    ctx          context.Context
    etcdClient   *clientv3.Client
    limit        int64
    keyPrefix    string
    stopDuration time.Duration
}

// NodeID 设置节点ID
func NodeID(id string) NodeOption {
    return func(o *nodeOptions) { o.id = id }
}

// NodeName 设置节点名称
func NodeName(name string) NodeOption {
    return func(o *nodeOptions) { o.name = name }
}

// NodeVersion 设置节点版本
func NodeVersion(version string) NodeOption {
    return func(o *nodeOptions) { o.version = version }
}

// NodeAddr 设置节点地址
func NodeAddr(addr string) NodeOption {
    return func(o *nodeOptions) { o.addr = addr }
}

// NodeCtx 设置节点上下文
func NodeCtx(ctx context.Context) NodeOption {
    return func(o *nodeOptions) { o.ctx = ctx }
}

// NodeEtcdClient 设置节点 etcd 客户端
func NodeEtcdClient(c *clientv3.Client) NodeOption {
    return func(o *nodeOptions) { o.etcdClient = c }
}

// NodeLimit 设置节点批量操作限制
func NodeLimit(limit int64) NodeOption {
    return func(o *nodeOptions) { o.limit = limit }
}

// NodeKeyPrefix 设置节点 KEY 前缀。
func NodeKeyPrefix(keyPrefix string) NodeOption {
    return func(o *nodeOptions) { o.keyPrefix = keyPrefix }
}

// NodeStopDuration 设置节点停止等待时间。
func NodeStopDuration(stopDuration time.Duration) NodeOption {
    return func(o *nodeOptions) { o.stopDuration = stopDuration }
}
