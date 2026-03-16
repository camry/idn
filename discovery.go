package idn

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"
    "sync"
    "sync/atomic"

    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/samber/lo"
    "github.com/spf13/cast"
    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

type Discovery[T IDType] struct {
    opts               *discoveryOptions            // 发现服务选项
    ctx                context.Context              // 发现服务上下文
    cancel             context.CancelFunc           // 发现服务取消函数
    leaseGrantResponse *clientv3.LeaseGrantResponse // 发现服务租约响应
    ncMap              sync.Map                     // 节点配置映射（节点 ID -> 节点配置）
    nGrpcMap           sync.Map                     // 节点 gRPC 客户端连接映射（节点 ID -> gRPC 客户端连接）
    imMap              sync.Map                     // ID 配置映射（ID -> 节点 ID）
}

// NewDiscovery 创建发现服务
func NewDiscovery[T IDType](opts ...DiscoveryOption) (*Discovery[T], error) {
    o := &discoveryOptions{
        ctx:        context.Background(),
        sessionTTL: 3,
        leaseTTL:   3600,
    }
    for _, opt := range opts {
        opt(o)
    }
    // 设置默认 gRPC 客户端连接选项
    if len(o.grpcDialOptions) == 0 {
        o.grpcDialOptions = []grpc.DialOption{
            grpc.WithTransportCredentials(insecure.NewCredentials()),
        }
    }
    if o.nodeName == "" {
        return nil, gerror.Newf("NewDiscovery NodeName %s is empty", o.nodeName)
    }
    if o.etcdClient == nil {
        return nil, gerror.Newf("NewDiscovery NodeName %s etcd client is nil", o.nodeName)
    }
    leaseGrantResponse, err := o.etcdClient.Grant(o.ctx, o.leaseTTL)
    if err != nil {
        return nil, gerror.Newf("NewDiscovery Grant: %v", err)
    }
    ctx, cancel := context.WithCancel(o.ctx)
    return &Discovery[T]{
        opts:               o,
        ctx:                ctx,
        cancel:             cancel,
        leaseGrantResponse: leaseGrantResponse,
    }, nil
}

// newGrpcClient 创建 gRPC 客户端连接
func (x *Discovery[T]) newGrpcClient(nc *nodeConfig) (*grpc.ClientConn, error) {
    grpcClient, err := grpc.NewClient(nc.Addr, x.opts.grpcDialOptions...)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery newGrpcClient NodeName %s grpc.NewClient Addr %s dial failed", x.opts.nodeName, nc.Addr)
    }
    return grpcClient, nil
}

// GrpcClient 获取 ID 对应的 gRPC 客户端连接
func (x *Discovery[T]) GrpcClient(id T) (*grpc.ClientConn, error) {
    if nodeId, ok := x.imMap.Load(id); ok {
        if grpcClient, ok2 := x.nGrpcMap.Load(cast.ToString(nodeId)); ok2 {
            return grpcClient.(*grpc.ClientConn), nil
        }
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s ID %v node %s not found", x.opts.nodeName, id, nodeId)
    }
    session, err := concurrency.NewSession(x.opts.etcdClient, concurrency.WithTTL(x.opts.sessionTTL))
    if err != nil {
        return nil, gerror.Wrapf(err, "NewDiscovery NodeName %s concurrency.NewSession failed", x.opts.nodeName)
    }
    defer session.Close()
    mu := concurrency.NewMutex(session, fmt.Sprintf(MutexMappingPrefix, x.opts.nodeName))
    if err := mu.Lock(x.ctx); err != nil {
        return nil, gerror.Wrapf(err, "Discovery GrpcClient NodeName %s ID %v lock etcd mutex failed", x.opts.nodeName, id)
    }
    defer mu.Unlock(x.ctx)
    if nodeId, ok := x.imMap.Load(id); ok {
        if grpcClient, ok2 := x.nGrpcMap.Load(cast.ToString(nodeId)); ok2 {
            return grpcClient.(*grpc.ClientConn), nil
        }
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s ID %v node %s mutex not found", x.opts.nodeName, id, nodeId)
    }
    // 从节点字典中获取负载最小的节点
    var minLoadNode *nodeConfig
    x.ncMap.Range(func(key, value any) bool {
        if nc, ok1 := value.(*nodeConfig); ok1 {
            if minLoadNode == nil || nc.Load < minLoadNode.Load {
                minLoadNode = nc
            }
        }
        return true
    })
    if minLoadNode == nil {
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s no available nodes", x.opts.nodeName)
    }
    grpcClient, ok := x.nGrpcMap.Load(minLoadNode.ID)
    if !ok {
        // 创建 gRPC 客户端连接
        grpcClientNew, err := x.newGrpcClient(minLoadNode)
        if err != nil {
            return nil, gerror.Wrapf(err, "Discovery GrpcClient NodeName %s ID %v node %s newGrpcClient failed", x.opts.nodeName, id, minLoadNode.ID)
        }
        grpcClient = grpcClientNew
        x.nGrpcMap.Store(minLoadNode.ID, grpcClient)
    }
    // 将更新后的 ID 映射配置写回 etcd
    _, err = x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(IdMappingKey, x.opts.nodeName, id), minLoadNode.ID, clientv3.WithLease(x.leaseGrantResponse.ID))
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery GrpcClient NodeName %s put node id %s etcd failed", x.opts.nodeName, minLoadNode.ID)
    }
    // 存储 ID 映射
    x.imMap.Store(id, minLoadNode.ID)
    // 更新对应节点的负载值
    atomic.AddInt32(&minLoadNode.Load, 1)
    glog.Debugf("Discovery GrpcClient NodeName %s ID %v node %s Load %d put etcd success %p", x.opts.nodeName, id, minLoadNode.ID, minLoadNode.Load, minLoadNode)
    return grpcClient.(*grpc.ClientConn), nil
}

// Start 启动发现服务
func (x *Discovery[T]) Start(_ context.Context) error {
    if x.opts.etcdClient == nil {
        return gerror.Newf("Discovery Start NodeName %s etcd client is nil", x.opts.nodeName)
    }
    // 初始化所有服务节点和节点 gRPC 客户端连接映射
    resp, err := x.opts.etcdClient.Get(x.ctx, fmt.Sprintf(DiscoveryServicesPrefix, x.opts.nodeName), clientv3.WithPrefix())
    if err != nil {
        return gerror.Wrapf(err, "Discovery Start NodeName %s DiscoveryServicesPrefix Get etcd error", x.opts.nodeName)
    }
    for _, kv := range resp.Kvs {
        nc := &nodeConfig{}
        err = json.Unmarshal(kv.Value, nc)
        if err != nil {
            return gerror.Wrapf(err, "Discovery Start NodeName %s DiscoveryServicesPrefix Key %s Value %s Unmarshal etcd failed", x.opts.nodeName, kv.Key, kv.Value)
        }
        grpcClient, err1 := x.newGrpcClient(nc)
        if err1 != nil {
            return gerror.Wrapf(err1, "Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Value %s newGrpcClient failed", x.opts.nodeName, kv.Key, kv.Value)
        }
        x.nGrpcMap.Store(nc.ID, grpcClient)
        x.ncMap.Store(nc.ID, nc)
        glog.Debugf("Discovery Start NodeName %s DiscoveryServicesPrefix Key %s Value %s", x.opts.nodeName, kv.Key, kv.Value)
    }
    // 初始化所有 ID 映射
    resp, err = x.opts.etcdClient.Get(x.ctx, fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.nodeName), clientv3.WithPrefix())
    if err != nil {
        return gerror.Wrapf(err, "Discovery Start NodeName %s DiscoveryMappingsPrefix Get etcd failed", x.opts.nodeName)
    }
    for _, kv := range resp.Kvs {
        idV, ok := lo.Last(strings.Split(string(kv.Key), "/"))
        if !ok {
            return gerror.Newf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Last Split failed", x.opts.nodeName, kv.Key)
        }
        id := cast.To[T](idV)
        nodeId := cast.ToString(kv.Value)
        ncV, ok := x.ncMap.Load(nodeId)
        if !ok {
            return gerror.Newf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Value %s node %s not found", x.opts.nodeName, kv.Key, kv.Value, nodeId)
        }
        nc, ok := ncV.(*nodeConfig)
        if !ok {
            return gerror.Newf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Value %s node %s not match", x.opts.nodeName, kv.Key, kv.Value, nodeId)
        }
        if nc.ID == nodeId {
            atomic.AddInt32(&nc.Load, 1)
        }
        x.imMap.Store(id, nodeId)
        glog.Debugf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Value %s", x.opts.nodeName, kv.Key, kv.Value)
    }
    var wg sync.WaitGroup
    // 协程监听服务节点
    wg.Go(x.watchServiceNode)
    // 协程监听 ID 映射
    wg.Go(x.watchIDMapping)
    wg.Wait()
    return nil
}

// Stop 停止发现服务
func (x *Discovery[T]) Stop(_ context.Context) error {
    defer x.cancel()
    // 关闭所有 gRPC 客户端连接
    x.nGrpcMap.Range(func(key, value any) bool {
        if grpcClient, ok := value.(*grpc.ClientConn); ok {
            _ = grpcClient.Close()
        }
        return true
    })
    x.nGrpcMap.Clear()
    x.ncMap.Clear()
    x.imMap.Clear()
    return nil
}

// watchServiceNode 监听服务节点
func (x *Discovery[T]) watchServiceNode() {
    watchChan := x.opts.etcdClient.Watch(x.ctx, fmt.Sprintf(DiscoveryServicesPrefix, x.opts.nodeName), clientv3.WithPrefix())
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Discovery watchServiceNode NodeName %s Watch context canceled", x.opts.nodeName)
            return
        default:
            for response := range watchChan {
                for _, event := range response.Events {
                    switch event.Type {
                    case clientv3.EventTypePut:
                        glog.Debugf("Discovery watchServiceNode NodeName %s Key %s updated: %s", x.opts.nodeName, event.Kv.Key, event.Kv.Value)
                        // 处理键被添加或更新的逻辑
                        nodeId, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchServiceNode NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                            continue
                        }
                        _, ok = x.ncMap.Load(nodeId)
                        if ok {
                            continue
                        }
                        nc := &nodeConfig{}
                        err := json.Unmarshal(event.Kv.Value, nc)
                        if err != nil {
                            glog.Error(gerror.Wrapf(err, "Discovery watchServiceNode NodeName %s Key %s Value %s Unmarshal etcd failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                            continue
                        }
                        x.ncMap.Store(nc.ID, nc)
                        // 创建 gRPC 客户端连接
                        grpcClient, err := x.newGrpcClient(nc)
                        if err != nil {
                            glog.Error(gerror.Wrapf(err, "Discovery watchServiceNode NodeName %s Key %s Value %s newGrpcClient failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                            continue
                        }
                        x.nGrpcMap.Store(nc.ID, grpcClient)
                    case clientv3.EventTypeDelete:
                        glog.Debugf("Discovery watchServiceNode NodeName %s Key %s deleted", x.opts.nodeName, event.Kv.Key)
                        // 处理键被删除的逻辑
                        nodeId, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchServiceNode NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                            continue
                        }
                        x.imMap.Range(func(key, value any) bool {
                            idK := cast.To[T](key)
                            nodeIdV := cast.ToString(value)
                            if nodeIdV == nodeId {
                                x.imMap.Delete(idK)
                            }
                            return true
                        })
                        if grpcClient, ok1 := x.nGrpcMap.Load(nodeId); ok1 {
                            if conn, ok2 := grpcClient.(*grpc.ClientConn); ok2 {
                                _ = conn.Close()
                            }
                            x.nGrpcMap.Delete(nodeId)
                        }
                        x.ncMap.Delete(nodeId)
                    }
                }
            }
        }
    }
}

// watchIDMapping 监听 ID 映射
func (x *Discovery[T]) watchIDMapping() {
    watchChan := x.opts.etcdClient.Watch(x.ctx, fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.nodeName), clientv3.WithPrefix())
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Discovery watchIDMapping NodeName %s Watch context canceled", x.opts.nodeName)
            return
        default:
            for response := range watchChan {
                for _, event := range response.Events {
                    switch event.Type {
                    case clientv3.EventTypePut:
                        glog.Debugf("Discovery watchIDMapping NodeName %s Key %s updated: %s", x.opts.nodeName, event.Kv.Key, event.Kv.Value)
                        // 处理键被添加或更新的逻辑
                        idV, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchIDMapping NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                            continue
                        }
                        id := cast.To[T](idV)
                        nodeId := cast.ToString(event.Kv.Value)
                        ncV, ok := x.ncMap.Load(nodeId)
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchIDMapping NodeName %s Key %s Value %s Load NodeConfig not found", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                            continue
                        }
                        nc, ok := ncV.(*nodeConfig)
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchIDMapping NodeName %s Key %s Value %s Load NodeConfig not match", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                            continue
                        }
                        _, ok = x.nGrpcMap.Load(nodeId)
                        if !ok {
                            grpcClient, err := x.newGrpcClient(nc)
                            if err != nil {
                                glog.Error(gerror.Wrapf(err, "Discovery watchIDMapping NodeName %s Key %s Value %s newGrpcClient failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                                continue
                            }
                            x.nGrpcMap.Store(nodeId, grpcClient)
                        }
                        _, ok = x.imMap.Load(id)
                        if !ok {
                            atomic.AddInt32(&nc.Load, 1)
                            x.imMap.Store(id, nodeId)
                            glog.Debugf("Discovery watchIDMapping NodeName %s Key %s Value %s Load %d", x.opts.nodeName, event.Kv.Key, event.Kv.Value, nc.Load)
                        }
                    case clientv3.EventTypeDelete:
                        glog.Debugf("Discovery watchIDMapping NodeName %s Key %s deleted", x.opts.nodeName, event.Kv.Key)
                        // 处理键被删除的逻辑
                        idV, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                        if !ok {
                            glog.Error(gerror.Newf("Discovery watchIDMapping NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                            continue
                        }
                        id := cast.To[T](idV)
                        x.imMap.Delete(id)
                    }
                }
            }
        }
    }
}
