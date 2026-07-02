package idn

import (
    "context"
    "time"

    "github.com/redis/go-redis/v9"
    clientv3 "go.etcd.io/etcd/client/v3"
    "google.golang.org/grpc"
)

type DiscoveryOption func(*discoveryOptions)

type discoveryOptions struct {
    project         string
    nodeName        string
    ctx             context.Context
    etcdClient      *clientv3.Client
    redisClient     *redis.Client
    grpcDialOptions []grpc.DialOption
    minLoadNodeTTL  time.Duration
}

// DiscoveryProject 设置发现服务项目名称
func DiscoveryProject(project string) DiscoveryOption {
    return func(o *discoveryOptions) { o.project = project }
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

// DiscoveryRedisClient 设置发现服务 redis 客户端
func DiscoveryRedisClient(c *redis.Client) DiscoveryOption {
    return func(o *discoveryOptions) { o.redisClient = c }
}

// DiscoveryGrpcDialOptions 设置发现服务 gRPC 客户端连接选项
func DiscoveryGrpcDialOptions(opts ...grpc.DialOption) DiscoveryOption {
    return func(o *discoveryOptions) { o.grpcDialOptions = opts }
}

// DiscoveryMinLoadNodeTTL 设置发现服务最小负载节点 TTL
func DiscoveryMinLoadNodeTTL(ttl time.Duration) DiscoveryOption {
    return func(o *discoveryOptions) { o.minLoadNodeTTL = ttl }
}
