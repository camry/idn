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
    sessionTTL      int
    leaseTTL        int64
    isNodeBind      bool
    limit           int64
    keyPrefix       string
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

// DiscoverySessionTTL 设置发现服务会话 TTL (单位: 秒)
func DiscoverySessionTTL(ttl int) DiscoveryOption {
    return func(o *discoveryOptions) { o.sessionTTL = ttl }
}

// DiscoveryLeaseTTL 设置发现服务租约 TTL (单位: 秒)
func DiscoveryLeaseTTL(ttl int64) DiscoveryOption {
    return func(o *discoveryOptions) { o.leaseTTL = ttl }
}

// DiscoveryIsNodeBind 设置发现服务是否节点绑定
func DiscoveryIsNodeBind(isNodeBind bool) DiscoveryOption {
    return func(o *discoveryOptions) { o.isNodeBind = isNodeBind }
}

// DiscoveryLimit 设置发现服务批量操作限制
func DiscoveryLimit(limit int64) DiscoveryOption {
    return func(o *discoveryOptions) { o.limit = limit }
}

// DiscoveryKeyPrefix 设置发现服务 KEY 前缀。
func DiscoveryKeyPrefix(keyPrefix string) DiscoveryOption {
    return func(o *discoveryOptions) { o.keyPrefix = keyPrefix }
}
