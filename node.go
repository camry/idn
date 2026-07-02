package idn

import (
    "context"
    "encoding/json"
    "fmt"
    "sync"
    "sync/atomic"
    "time"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/google/uuid"
    "github.com/redis/go-redis/v9"
    clientv3 "go.etcd.io/etcd/client/v3"
)

type Node[T IDType] struct {
    opts               *nodeOptions                            // 节点选项
    ctx                context.Context                         // 节点上下文
    cancel             context.CancelFunc                      // 节点取消函数
    leaseGrantResponse *clientv3.LeaseGrantResponse            // 节点租约响应
    leaseKeepAliveChan <-chan *clientv3.LeaseKeepAliveResponse // 节点租约续期通道
    isStopped          atomic.Bool                             // 节点是否停止
}

// NewNode 创建节点
func NewNode[T IDType](opts ...NodeOption) (*Node[T], error) {
    o := &nodeOptions{
        ctx:      context.Background(),
        project:  "wild",
        name:     "uid",
        leaseTTL: 30,
    }
    if id, err := uuid.NewUUID(); err == nil {
        o.id = id.String()
    }
    for _, opt := range opts {
        opt(o)
    }
    if o.id == "" {
        return nil, gerror.Newf("NewNode id can not be empty")
    }
    if o.name == "" {
        return nil, gerror.Newf("NewNode ID %s NodeName is empty", o.id)
    }
    if o.etcdClient == nil {
        return nil, gerror.Newf("NewNode ID %s NodeName %s etcd client is nil", o.id, o.name)
    }
    if o.redisClient == nil {
        return nil, gerror.Newf("NewNode ID %s NodeName %s redis client is nil", o.id, o.name)
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
    return x.opts.grpcAddr
}

// Start 启动节点
func (x *Node[T]) Start(_ context.Context) error {
    glog.Infof("[NODE] %s server starting", x.Name())
    // 创建租约并启动自动续期
    leaseGrantResponse, err := x.opts.etcdClient.Grant(x.ctx, x.opts.leaseTTL)
    if err != nil {
        return gerror.Wrapf(err, "Node Start NodeName %s etcdClient.Grant failed", x.Name())
    }
    succeeded := false
    defer func() {
        if !succeeded {
            if _, rErr := x.opts.etcdClient.Revoke(context.Background(), leaseGrantResponse.ID); rErr != nil {
                glog.Warn(gerror.Wrapf(rErr, "[NODE] Start cleanup Revoke lease %d failed", leaseGrantResponse.ID))
            }
        }
    }()
    // 启动租约自动续期
    leaseKeepAliveChan, err := x.opts.etcdClient.KeepAlive(x.ctx, leaseGrantResponse.ID)
    if err != nil {
        return gerror.Wrapf(err, "Node Start NodeName %s etcdClient.KeepAlive failed", x.Name())
    }
    // 注册节点
    nc := &nodeConfig{
        ID:          x.opts.id,
        Name:        x.opts.name,
        Version:     x.opts.version,
        BuildTime:   x.opts.buildTime,
        BuildCommit: x.opts.buildCommit,
        HTTPAddr:    x.opts.httpAddr,
        GRPCAddr:    x.opts.grpcAddr,
        TCPAddr:     x.opts.tcpAddr,
    }
    v, err := json.Marshal(nc)
    if err != nil {
        return gerror.Wrapf(err, "Node ID %s NodeName %s Start marshal node config failed", x.opts.id, x.opts.name)
    }
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(ServiceKey, x.opts.project, x.opts.name, x.opts.id), string(v), clientv3.WithLease(leaseGrantResponse.ID))
    if err != nil {
        return gerror.Wrapf(err, "Node ID %s NodeName %s Start register to etcd failed", x.opts.id, x.opts.name)
    }
    // 注册节点ID对应的负载值
    err = x.opts.redisClient.HSet(x.ctx, fmt.Sprintf(ServiceLoadKey, x.opts.project, x.opts.name), x.opts.id, 0).Err()
    if err != nil {
        return gerror.Wrapf(err, "Node ID %s NodeName %s Start register to redis failed", x.opts.id, x.opts.name)
    }
    x.leaseGrantResponse = leaseGrantResponse
    x.leaseKeepAliveChan = leaseKeepAliveChan
    succeeded = true
    var wg sync.WaitGroup
    // 协程监听消费租约续期响应，防止阻塞
    wg.Go(x.watchLeaseKeepAlive)
    wg.Wait()
    return nil
}

// Stop 停止节点
func (x *Node[T]) Stop(_ context.Context) error {
    glog.Infof("[NODE] %s server stopping", x.Name())
    x.isStopped.Store(true)
    defer x.cancel()
    // 撤销租约
    if x.leaseGrantResponse != nil {
        if _, err := x.opts.etcdClient.Revoke(x.ctx, x.leaseGrantResponse.ID); err != nil {
            glog.Warn(gerror.Wrapf(err, "[NODE] Stop etcdClient.Revoke lease %d failed", x.leaseGrantResponse.ID))
        }
        x.leaseGrantResponse = nil
        x.leaseKeepAliveChan = nil
    }
    // 注销节点ID对应的负载值
    err := x.opts.redisClient.HDel(x.ctx, fmt.Sprintf(ServiceLoadKey, x.opts.project, x.opts.name), x.opts.id).Err()
    if err != nil {
        return gerror.Wrapf(err, "Node ID %s NodeName %s Stop unregister from redis failed", x.opts.id, x.opts.name)
    }
    return nil
}

// BindID 绑定ID
func (x *Node[T]) BindID(idName string, id T, ttl time.Duration) error {
    if x.isStopped.Load() {
        return gerror.Newf("Node BindID %s ID %v NodeID %s NodeName %s is stopped", idName, id, x.opts.id, x.opts.name)
    }
    _, err := x.opts.redisClient.TxPipelined(x.ctx, func(pipeliner redis.Pipeliner) error {
        err := pipeliner.Set(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.project, x.opts.name, idName, id), x.opts.id, ttl).Err()
        if err != nil {
            return err
        }
        err = pipeliner.HIncrBy(x.ctx, fmt.Sprintf(ServiceLoadKey, x.opts.project, x.opts.name), x.opts.id, 1).Err()
        if err != nil {
            return err
        }
        return nil
    })
    if err != nil {
        return gerror.Wrapf(err, "Node BindID %s ID %v NodeID %s NodeName %s failed", idName, id, x.opts.id, x.opts.name)
    }
    return nil
}

// UnbindID 解绑ID
func (x *Node[T]) UnbindID(idName string, id T) error {
    if x.isStopped.Load() {
        return gerror.Newf("Node UnbindID %s ID %v NodeID %s NodeName %s is stopped", idName, id, x.opts.id, x.opts.name)
    }
    _, err := x.opts.redisClient.TxPipelined(x.ctx, func(pipeliner redis.Pipeliner) error {
        err := pipeliner.Del(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.project, x.opts.name, idName, id)).Err()
        if err != nil {
            return err
        }
        err = pipeliner.HIncrBy(x.ctx, fmt.Sprintf(ServiceLoadKey, x.opts.project, x.opts.name), x.opts.id, -1).Err()
        if err != nil {
            return err
        }
        return nil //
    })
    if err != nil {
        return gerror.Wrapf(err, "Node UnbindID %s ID %v NodeID %s NodeName %s failed", idName, id, x.opts.id, x.opts.name)
    }
    return nil
}

// HeartbeatID 心跳ID
func (x *Node[T]) HeartbeatID(idName string, id T, ttl time.Duration) error {
    if x.isStopped.Load() {
        return gerror.Newf("Node HeartbeatID %s ID %v NodeID %s NodeName %s is stopped", idName, id, x.opts.id, x.opts.name)
    }
    err := x.opts.redisClient.Expire(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.project, x.opts.name, idName, id), ttl).Err()
    if err != nil {
        return gerror.Wrapf(err, "Node HeartbeatID %s ID %v NodeID %s NodeName %s failed", idName, id, x.opts.id, x.opts.name)
    }
    return nil
}

// HasID 检查ID是否存在
func (x *Node[T]) HasID(idName string, id T) (bool, error) {
    exists, err := x.opts.redisClient.Exists(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.project, x.opts.name, idName, id)).Result()
    if err != nil {
        return false, gerror.Wrapf(err, "Node HasID %s ID %v NodeID %s NodeName %s failed", idName, id, x.opts.id, x.opts.name)
    }
    return exists > 0, nil
}

// watchLeaseKeepAlive 监听租约续期状态
func (x *Node[T]) watchLeaseKeepAlive() {
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Node watchLeaseKeepAlive NodeName %s context canceled", x.Name())
            return
        case _, ok := <-x.leaseKeepAliveChan:
            if !ok {
                glog.Warnf("Node watchLeaseKeepAlive NodeName %s lease keepalive channel closed", x.Name())
                return
            }
            // glog.Debugf("Node watchLeaseKeepAlive NodeName %s lease %d TTL %d", x.Name(), resp.ID, resp.TTL)
        }
    }
}
