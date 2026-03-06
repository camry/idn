package sdn

import (
    "context"

    clientv3 "go.etcd.io/etcd/client/v3"
)

type NodeOption func(o *nodeOptions)

type nodeOptions struct {
    id         string
    name       string
    version    string
    addr       string
    ctx        context.Context
    etcdClient *clientv3.Client
}

// ID 设置节点ID
func ID(id string) NodeOption {
    return func(o *nodeOptions) { o.id = id }
}

// Name 设置节点名称
func Name(name string) NodeOption {
    return func(o *nodeOptions) { o.name = name }
}

// Version 设置节点版本
func Version(version string) NodeOption {
    return func(o *nodeOptions) { o.version = version }
}

// Addr 设置节点地址
func Addr(addr string) NodeOption {
    return func(o *nodeOptions) { o.addr = addr }
}

// Ctx 设置节点上下文
func Ctx(ctx context.Context) NodeOption {
    return func(o *nodeOptions) { o.ctx = ctx }
}

// EtcdClient 设置节点 etcd 客户端
func EtcdClient(c *clientv3.Client) NodeOption {
    return func(o *nodeOptions) { o.etcdClient = c }
}
