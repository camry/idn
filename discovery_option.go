package idn

import (
    "context"

    clientv3 "go.etcd.io/etcd/client/v3"
    "google.golang.org/grpc"
)

type DiscoveryOption func(*discoveryOptions)

type discoveryOptions struct {
    nodeName        string
    ctx             context.Context
    etcdClient      *clientv3.Client
    grpcDialOptions []grpc.DialOption
}

// DiscoveryNodeName 设置发现服务节点名称
func DiscoveryNodeName(name string) DiscoveryOption {
    return func(o *discoveryOptions) { o.nodeName = name }
}

// DiscoveryCtx 设置发现服务上下文
func DiscoveryCtx(ctx context.Context) DiscoveryOption {
    return func(o *discoveryOptions) { o.ctx = ctx }
}

// DiscoveryEtcdClient 设置发现服务 etcd 客户端
func DiscoveryEtcdClient(c *clientv3.Client) DiscoveryOption {
    return func(o *discoveryOptions) { o.etcdClient = c }
}

// DiscoveryGrpcDialOptions 设置发现服务 gRPC 客户端连接选项
func DiscoveryGrpcDialOptions(opts ...grpc.DialOption) DiscoveryOption {
    return func(o *discoveryOptions) { o.grpcDialOptions = opts }
}
