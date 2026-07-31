package idn

import (
    "context"
    "time"

    "github.com/redis/go-redis/v9"
    clientv3 "go.etcd.io/etcd/client/v3"
)

type NodeOption func(o *nodeOptions)

type nodeOptions struct {
    id                   string
    project              string
    name                 string
    version              string
    buildTime            string
    buildCommit          string
    httpAddr             string
    grpcAddr             string
    tcpAddr              string
    ctx                  context.Context
    etcdClient           *clientv3.Client
    redisClient          *redis.Client
    leaseTTL             int64
    batchOpFlushInterval time.Duration
    batchOpSize          int
    batchOpChanSize      int
}

// NodeID 设置节点ID
func NodeID(id string) NodeOption {
    return func(o *nodeOptions) { o.id = id }
}

// NodeProject 设置节点项目
func NodeProject(project string) NodeOption {
    return func(o *nodeOptions) { o.project = project }
}

// NodeName 设置节点名称
func NodeName(name string) NodeOption {
    return func(o *nodeOptions) { o.name = name }
}

// NodeVersion 设置节点版本
func NodeVersion(version string) NodeOption {
    return func(o *nodeOptions) { o.version = version }
}

// NodeBuildTime 设置节点构建时间
func NodeBuildTime(buildTime string) NodeOption {
    return func(o *nodeOptions) { o.buildTime = buildTime }
}

// NodeBuildCommit 设置节点构建提交
func NodeBuildCommit(buildCommit string) NodeOption {
    return func(o *nodeOptions) { o.buildCommit = buildCommit }
}

// NodeHTTPAddr 设置节点 HTTP 地址
func NodeHTTPAddr(httpAddr string) NodeOption {
    return func(o *nodeOptions) { o.httpAddr = httpAddr }
}

// NodeGRPCAddr 设置节点 GRPC 地址
func NodeGRPCAddr(grpcAddr string) NodeOption {
    return func(o *nodeOptions) { o.grpcAddr = grpcAddr }
}

// NodeTCPAddr 设置节点 TCP 地址
func NodeTCPAddr(tcpAddr string) NodeOption {
    return func(o *nodeOptions) { o.tcpAddr = tcpAddr }
}

// NodeCtx 设置节点上下文
func NodeCtx(ctx context.Context) NodeOption {
    return func(o *nodeOptions) { o.ctx = ctx }
}

// NodeEtcdClient 设置节点 etcd 客户端
func NodeEtcdClient(c *clientv3.Client) NodeOption {
    return func(o *nodeOptions) { o.etcdClient = c }
}

// NodeRedisClient 设置节点 redis 客户端
func NodeRedisClient(c *redis.Client) NodeOption {
    return func(o *nodeOptions) { o.redisClient = c }
}

// NodeLeaseTTL 设置节点租约 TTL。
func NodeLeaseTTL(leaseTTL int64) NodeOption {
    return func(o *nodeOptions) { o.leaseTTL = leaseTTL }
}

// NodeBatchOpFlushInterval 设置批聚合刷新时间间隔，默认 5ms。
func NodeBatchOpFlushInterval(batchOpFlushInterval time.Duration) NodeOption {
    return func(o *nodeOptions) { o.batchOpFlushInterval = batchOpFlushInterval }
}

// NodeBatchOpSize 设置单批最大条数，达到后立即刷新，默认 500。
func NodeBatchOpSize(batchOpSize int) NodeOption {
    return func(o *nodeOptions) { o.batchOpSize = batchOpSize }
}

// NodeBatchOpChanSize 设置批聚合 channel 缓冲大小，默认 10000。
func NodeBatchOpChanSize(batchOpChanSize int) NodeOption {
    return func(o *nodeOptions) { o.batchOpChanSize = batchOpChanSize }
}
