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
    "github.com/camry/g/v2/gos/mutex"
    "github.com/samber/lo"
    "github.com/spf13/cast"
    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

type Discovery[T IDType] struct {
    opts               *discoveryOptions                       // 发现服务选项
    ctx                context.Context                         // 发现服务上下文
    cancel             context.CancelFunc                      // 发现服务取消函数
    mu                 *mutex.Mutex                            // 发现服务互斥锁
    session            *concurrency.Session                    // 发现服务会话
    leaseGrantResponse *clientv3.LeaseGrantResponse            // 发现服务租约响应
    leaseKeepAliveChan <-chan *clientv3.LeaseKeepAliveResponse // 发现服务租约续期通道
    ncMap              sync.Map                                // 节点配置映射 (节点ID -> 节点配置)
    nGrpcMap           sync.Map                                // 节点 gRPC 客户端连接映射 (节点ID -> gRPC 客户端连接)
    inMap              sync.Map                                // ID 配置映射 (ID -> 节点ID)
    niMap              sync.Map                                // 节点配置映射 (节点ID -> sync.Map[ID]struct{}{})
    minLoadNodePtr     atomic.Pointer[nodeConfig]              // 缓存最小负载节点
}

// NewDiscovery 创建发现服务
func NewDiscovery[T IDType](opts ...DiscoveryOption) (*Discovery[T], error) {
    o := &discoveryOptions{
        ctx:      context.Background(),
        leaseTTL: 30,
        limit:    DefaultLimit,
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
    ctx, cancel := context.WithCancel(o.ctx)
    discovery := &Discovery[T]{
        opts:   o,
        ctx:    ctx,
        cancel: cancel,
        mu:     mutex.New(true),
    }
    return discovery, nil
}

// newGrpcClient 创建 gRPC 客户端连接
func (x *Discovery[T]) newGrpcClient(nc *nodeConfig) (*grpc.ClientConn, error) {
    grpcClient, err := grpc.NewClient(nc.GetGRPCAddr(), x.opts.grpcDialOptions...)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery newGrpcClient NodeName %s grpc.NewClient Addr %s dial failed", x.opts.nodeName, nc.GetGRPCAddr())
    }
    return grpcClient, nil
}

// IDNode 获取 ID 对应的节点配置
func (x *Discovery[T]) IDNode(id T) (INodeConfig, bool) {
    nodeID, ok := x.inMap.Load(id)
    if !ok {
        return nil, false
    }
    nc, ok := x.ncMap.Load(cast.ToString(nodeID))
    if !ok {
        return nil, false
    }
    return nc.(*nodeConfig), ok
}

// minLoadNode 获取负载最小的节点
func (x *Discovery[T]) minLoadNode() (*nodeConfig, bool) {
    cacheMinLoadNode := x.minLoadNodePtr.Load()
    if cacheMinLoadNode != nil {
        return cacheMinLoadNode, true
    }
    var minLoadNode *nodeConfig
    x.ncMap.Range(func(key, value any) bool {
        if nc, ok1 := value.(*nodeConfig); ok1 {
            if minLoadNode == nil || nc.Load.Load() < minLoadNode.Load.Load() {
                minLoadNode = nc
            }
        }
        return true
    })
    if minLoadNode == nil {
        return nil, false
    }
    x.minLoadNodePtr.Store(minLoadNode)
    return minLoadNode, true
}

// MinLoadNode 获取负载最小的节点
func (x *Discovery[T]) MinLoadNode() (INodeConfig, bool) {
    return x.minLoadNode()
}

// GrpcClient 获取 ID 对应的 gRPC 客户端连接
func (x *Discovery[T]) GrpcClient(id T) (*grpc.ClientConn, error) {
    if nodeID, ok := x.inMap.Load(id); ok {
        if grpcClient, ok2 := x.nGrpcMap.Load(cast.ToString(nodeID)); ok2 {
            return grpcClient.(*grpc.ClientConn), nil
        }
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s ID %v node %s not found", x.opts.nodeName, id, nodeID)
    }
    x.mu.Lock()
    defer x.mu.Unlock()
    if nodeID, ok := x.inMap.Load(id); ok {
        if grpcClient, ok2 := x.nGrpcMap.Load(cast.ToString(nodeID)); ok2 {
            return grpcClient.(*grpc.ClientConn), nil
        }
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s ID %v node %s not found", x.opts.nodeName, id, nodeID)
    }
    if x.opts.isNodeBind {
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s ID %v not found", x.opts.nodeName, id)
    }
    mu := concurrency.NewMutex(x.session, fmt.Sprintf(MutexMappingPrefix, x.opts.keyPrefix, x.opts.nodeName))
    if err := mu.Lock(x.ctx); err != nil {
        return nil, gerror.Wrapf(err, "Discovery GrpcClient NodeName %s ID %v lock etcd mutex failed", x.opts.nodeName, id)
    }
    defer mu.Unlock(x.ctx)
    if nodeID, ok := x.inMap.Load(id); ok {
        if grpcClient, ok2 := x.nGrpcMap.Load(cast.ToString(nodeID)); ok2 {
            return grpcClient.(*grpc.ClientConn), nil
        }
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s ID %v node %s mutex not found", x.opts.nodeName, id, nodeID)
    }
    // 获取负载最小的节点
    minLoadNode, ok := x.minLoadNode()
    if !ok {
        return nil, gerror.Newf("Discovery GrpcClient NodeName %s no available nodes", x.opts.nodeName)
    }
    grpcClient, ok := x.nGrpcMap.Load(minLoadNode.ID)
    if !ok {
        // 创建 gRPC 客户端连接
        grpcClientNew, err1 := x.newGrpcClient(minLoadNode)
        if err1 != nil {
            return nil, gerror.Wrapf(err1, "Discovery GrpcClient NodeName %s ID %v node %s newGrpcClient failed", x.opts.nodeName, id, minLoadNode.ID)
        }
        grpcClient = grpcClientNew
        x.nGrpcMap.Store(minLoadNode.ID, grpcClient)
    }
    // 将更新后的 ID 映射配置写回 etcd
    opOptions := []clientv3.OpOption{
        clientv3.WithLease(x.leaseGrantResponse.ID),
    }
    _, err := x.opts.etcdClient.Put(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.keyPrefix, x.opts.nodeName, id), minLoadNode.ID, opOptions...)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery GrpcClient NodeName %s put node id %s etcd failed", x.opts.nodeName, minLoadNode.ID)
    }
    // 存储 ID 映射
    niMapID, _ := x.niMap.LoadOrStore(minLoadNode.ID, &sync.Map{})
    niMapID.(*sync.Map).Store(id, struct{}{})
    x.inMap.Store(id, minLoadNode.ID)
    // 更新对应节点的负载值
    minLoadNode.Load.Add(1)
    x.minLoadNodePtr.Store(nil)
    glog.Debugf("Discovery GrpcClient NodeName %s ID %v node %s Load %d put etcd success %p", x.opts.nodeName, id, minLoadNode.ID, minLoadNode.Load.Load(), minLoadNode)
    return grpcClient.(*grpc.ClientConn), nil
}

// Start 启动发现服务
func (x *Discovery[T]) Start(_ context.Context) error {
    glog.Infof("[DISCOVERY] node %s server starting", x.opts.nodeName)

    if x.opts.etcdClient == nil {
        return gerror.Newf("Discovery Start NodeName %s etcd client is nil", x.opts.nodeName)
    }

    // 创建租约并启动自动续期
    leaseGrantResponse, err := x.opts.etcdClient.Grant(x.ctx, x.opts.leaseTTL)
    if err != nil {
        return gerror.Wrapf(err, "Discovery Start NodeName %s etcdClient.Grant failed", x.opts.nodeName)
    }
    x.leaseGrantResponse = leaseGrantResponse
    // 启动租约自动续期
    leaseKeepAliveChan, err := x.opts.etcdClient.KeepAlive(x.ctx, leaseGrantResponse.ID)
    if err != nil {
        return gerror.Wrapf(err, "Discovery Start NodeName %s etcdClient.KeepAlive failed", x.opts.nodeName)
    }
    x.leaseKeepAliveChan = leaseKeepAliveChan
    // 创建会话
    session, err := concurrency.NewSession(x.opts.etcdClient, concurrency.WithLease(x.leaseGrantResponse.ID))
    if err != nil {
        return gerror.Wrapf(err, "Discovery Start NodeName %s concurrency.NewSession failed", x.opts.nodeName)
    }
    x.session = session

    var wg sync.WaitGroup
    // 协程监听消费租约续期响应，防止阻塞
    wg.Go(x.watchLeaseKeepAlive)
    // 协程监听服务节点
    wg.Go(x.watchServiceNode)
    // 协程监听 ID 映射
    wg.Go(x.watchIDMapping)
    // 初始化所有服务节点和节点 gRPC 客户端连接映射
    resp, err := x.opts.etcdClient.Get(x.ctx, fmt.Sprintf(DiscoveryServicesPrefix, x.opts.keyPrefix, x.opts.nodeName), clientv3.WithPrefix())
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
    discoveryMappingsPrefixKey := fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.keyPrefix, x.opts.nodeName)
    startKey := discoveryMappingsPrefixKey
    endKey := clientv3.GetPrefixRangeEnd(discoveryMappingsPrefixKey)
    for {
        resp, err = x.opts.etcdClient.Get(x.ctx, startKey,
            clientv3.WithLimit(x.opts.limit),
            clientv3.WithRange(endKey),
            clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
        )
        if err != nil {
            return gerror.Wrapf(err, "Discovery Start NodeName %s DiscoveryMappingsPrefix Get etcd failed", x.opts.nodeName)
        }
        for _, kv := range resp.Kvs {
            idV, ok := lo.Last(strings.Split(string(kv.Key), "/"))
            if !ok {
                return gerror.Newf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Last Split failed", x.opts.nodeName, kv.Key)
            }
            id := cast.To[T](idV)
            nodeID := cast.ToString(kv.Value)
            ncV, ok := x.ncMap.Load(nodeID)
            if !ok {
                return gerror.Newf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Value %s node %s not found", x.opts.nodeName, kv.Key, kv.Value, nodeID)
            }
            nc, ok := ncV.(*nodeConfig)
            if !ok {
                return gerror.Newf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Value %s node %s not match", x.opts.nodeName, kv.Key, kv.Value, nodeID)
            }
            _, ok = x.inMap.Load(id)
            if !ok {
                nc.Load.Add(1)
                x.minLoadNodePtr.Store(nil)
                niMapID, _ := x.niMap.LoadOrStore(nodeID, &sync.Map{})
                niMapID.(*sync.Map).Store(id, struct{}{})
                x.inMap.Store(id, nodeID)
            }
            glog.Debugf("Discovery Start NodeName %s DiscoveryMappingsPrefix Key %s Value %s", x.opts.nodeName, kv.Key, kv.Value)
        }
        if !resp.More {
            break
        }
        lastKV, ok := lo.Last(resp.Kvs)
        if ok {
            startKey = string(append(lastKV.Key, 0))
        }
    }
    wg.Wait()
    return nil
}

// Stop 停止发现服务
func (x *Discovery[T]) Stop(_ context.Context) error {
    glog.Infof("[DISCOVERY] Stop node %s server stopping", x.opts.nodeName)
    defer x.cancel()

    // 撤销租约
    if x.leaseGrantResponse != nil {
        if _, err := x.opts.etcdClient.Revoke(x.ctx, x.leaseGrantResponse.ID); err != nil {
            glog.Warn(gerror.Wrapf(err, "[DISCOVERY] Stop etcdClient.Revoke lease %d failed", x.leaseGrantResponse.ID))
        }
        x.leaseGrantResponse = nil
        x.leaseKeepAliveChan = nil
    }
    // 关闭会话
    if x.session != nil {
        if err := x.session.Close(); err != nil {
            glog.Warn(gerror.Wrapf(err, "[DISCOVERY] Stop node %s session close failed", x.opts.nodeName))
        }
    }

    // 关闭所有 gRPC 客户端连接
    x.nGrpcMap.Range(func(key, value any) bool {
        if grpcClient, ok := value.(*grpc.ClientConn); ok {
            _ = grpcClient.Close()
        }
        return true
    })
    x.nGrpcMap.Clear()
    x.ncMap.Clear()
    x.inMap.Clear()
    x.niMap.Clear()
    return nil
}

// watchLeaseKeepAlive 监听租约续期状态
func (x *Discovery[T]) watchLeaseKeepAlive() {
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Discovery watchLeaseKeepAlive NodeName %s context canceled", x.opts.nodeName)
            return
        case _, ok := <-x.leaseKeepAliveChan:
            if !ok {
                glog.Debugf("Discovery watchLeaseKeepAlive NodeName %s lease keepalive channel closed", x.opts.nodeName)
                return
            }
            // glog.Debugf("Discovery watchLeaseKeepAlive NodeName %s lease %d TTL %d", x.opts.nodeName, resp.ID, resp.TTL)
        }
    }
}

// watchServiceNode 监听服务节点
func (x *Discovery[T]) watchServiceNode() {
    watchChan := x.opts.etcdClient.Watch(x.ctx, fmt.Sprintf(DiscoveryServicesPrefix, x.opts.keyPrefix, x.opts.nodeName), clientv3.WithPrefix())
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Discovery watchServiceNode NodeName %s Watch context canceled", x.opts.nodeName)
            return
        case response, cok := <-watchChan:
            if !cok {
                return
            }
            for _, event := range response.Events {
                switch event.Type {
                case clientv3.EventTypePut:
                    glog.Debugf("Discovery watchServiceNode EventTypePut NodeName %s Key %s updated: %s", x.opts.nodeName, event.Kv.Key, event.Kv.Value)
                    // 处理键被添加或更新的逻辑
                    nodeID, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                    if !ok {
                        glog.Error(gerror.Newf("Discovery watchServiceNode EventTypePut NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                        continue
                    }
                    _, ok = x.ncMap.Load(nodeID)
                    if ok {
                        continue
                    }
                    nc := &nodeConfig{}
                    err := json.Unmarshal(event.Kv.Value, nc)
                    if err != nil {
                        glog.Error(gerror.Wrapf(err, "Discovery watchServiceNode EventTypePut NodeName %s Key %s Value %s Unmarshal etcd failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                        continue
                    }
                    x.ncMap.Store(nc.ID, nc)
                    // 创建 gRPC 客户端连接
                    grpcClient, err := x.newGrpcClient(nc)
                    if err != nil {
                        glog.Error(gerror.Wrapf(err, "Discovery watchServiceNode EventTypePut NodeName %s Key %s Value %s newGrpcClient failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                        continue
                    }
                    x.nGrpcMap.Store(nc.ID, grpcClient)
                case clientv3.EventTypeDelete:
                    glog.Debugf("Discovery watchServiceNode EventTypeDelete NodeName %s Key %s deleted", x.opts.nodeName, event.Kv.Key)
                    // 处理键被删除的逻辑
                    nodeID, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                    if !ok {
                        glog.Error(gerror.Newf("Discovery watchServiceNode EventTypeDelete NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                        continue
                    }
                    niMap, ok := x.niMap.Load(nodeID)
                    if ok {
                        niMap.(*sync.Map).Range(func(key, value any) bool {
                            idK := cast.To[T](key)
                            x.inMap.Delete(idK)
                            return true
                        })
                    }
                    if grpcClient, ok1 := x.nGrpcMap.Load(nodeID); ok1 {
                        if conn, ok2 := grpcClient.(*grpc.ClientConn); ok2 {
                            _ = conn.Close()
                        }
                        x.nGrpcMap.Delete(nodeID)
                    }
                    x.ncMap.Delete(nodeID)
                    x.niMap.Delete(nodeID)
                    x.minLoadNodePtr.Store(nil)
                }
            }
        }
    }
}

// watchIDMapping 监听 ID 映射
func (x *Discovery[T]) watchIDMapping() {
    watchChan := x.opts.etcdClient.Watch(x.ctx, fmt.Sprintf(DiscoveryMappingsPrefix, x.opts.keyPrefix, x.opts.nodeName), clientv3.WithPrefix(), clientv3.WithPrevKV())
    for {
        select {
        case <-x.ctx.Done():
            glog.Debugf("Discovery watchIDMapping NodeName %s Watch context canceled", x.opts.nodeName)
            return
        case response, cok := <-watchChan:
            if !cok {
                return
            }
            for _, event := range response.Events {
                switch event.Type {
                case clientv3.EventTypePut:
                    glog.Debugf("Discovery watchIDMapping NodeName %s Key %s updated: %s", x.opts.nodeName, event.Kv.Key, event.Kv.Value)
                    // 处理键被添加或更新的逻辑
                    idV, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                    if !ok {
                        glog.Error(gerror.Newf("Discovery watchIDMapping EventTypePut NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                        continue
                    }
                    id := cast.To[T](idV)
                    nodeID := cast.ToString(event.Kv.Value)
                    ncV, ok := x.ncMap.Load(nodeID)
                    if !ok {
                        glog.Error(gerror.Newf("Discovery watchIDMapping EventTypePut NodeName %s Key %s Value %s Load NodeConfig not found", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                        continue
                    }
                    nc, ok := ncV.(*nodeConfig)
                    if !ok {
                        glog.Error(gerror.Newf("Discovery watchIDMapping EventTypePut NodeName %s Key %s Value %s Load NodeConfig not match", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                        continue
                    }
                    _, ok = x.nGrpcMap.Load(nodeID)
                    if !ok {
                        grpcClient, err := x.newGrpcClient(nc)
                        if err != nil {
                            glog.Error(gerror.Wrapf(err, "Discovery watchIDMapping EventTypePut NodeName %s Key %s Value %s newGrpcClient failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                            continue
                        }
                        x.nGrpcMap.Store(nodeID, grpcClient)
                    }
                    _, ok = x.inMap.Load(id)
                    if !ok {
                        nc.Load.Add(1)
                        x.minLoadNodePtr.Store(nil)
                        niMapID, _ := x.niMap.LoadOrStore(nodeID, &sync.Map{})
                        niMapID.(*sync.Map).Store(id, struct{}{})
                        x.inMap.Store(id, nodeID)
                        glog.Debugf("Discovery watchIDMapping NodeName %s Key %s Value %s Load %d", x.opts.nodeName, event.Kv.Key, event.Kv.Value, nc.Load.Load())
                    }
                case clientv3.EventTypeDelete:
                    glog.Debugf("Discovery watchIDMapping NodeName %s Key %s deleted", x.opts.nodeName, event.Kv.Key)
                    // 处理键被删除的逻辑
                    idV, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                    if !ok {
                        glog.Error(gerror.Newf("Discovery watchIDMapping EventTypeDelete NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                        continue
                    }
                    id := cast.To[T](idV)
                    nodeID := cast.ToString(event.PrevKv.Value)
                    x.inMap.Delete(id)
                    ncV, ok1 := x.ncMap.Load(nodeID)
                    nc, ok2 := ncV.(*nodeConfig)
                    if ok1 && ok2 {
                        nc.Load.Add(-1)
                    }
                    niMapID, ok := x.niMap.Load(nodeID)
                    if ok {
                        niMapID.(*sync.Map).Delete(id)
                    }
                }
            }
        }
    }
}
