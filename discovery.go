package idn

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "strings"
    "sync"
    "time"

    "github.com/bluele/gcache"
    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/redis/go-redis/v9"
    "github.com/samber/lo"
    "github.com/spf13/cast"
    clientv3 "go.etcd.io/etcd/client/v3"
    "golang.org/x/sync/singleflight"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/keepalive"
)

type Discovery[T IDType] struct {
    opts             *discoveryOptions  // 发现服务选项
    ctx              context.Context    // 发现服务上下文
    cancel           context.CancelFunc // 发现服务取消函数
    ncMap            sync.Map           // 节点配置映射 (节点ID -> 节点配置)
    nGrpcMap         sync.Map           // 节点 gRPC 客户端连接映射 (节点ID -> gRPC 客户端连接)
    minLoadNodeCache gcache.Cache       // 缓存最小负载节点
    sf               singleflight.Group
}

// NewDiscovery 创建发现服务
func NewDiscovery[T IDType](opts ...DiscoveryOption) (*Discovery[T], error) {
    o := &discoveryOptions{
        ctx:            context.Background(),
        project:        "wild",
        nodeName:       "uid",
        minLoadNodeTTL: time.Minute,
    }
    for _, opt := range opts {
        opt(o)
    }
    // 设置默认 gRPC 客户端连接选项
    if len(o.grpcDialOptions) == 0 {
        kp := keepalive.ClientParameters{
            // Time: 如果连接在 15 秒内没有任何活跃的 RPC 请求，则向客户端发送一个 Ping 探测包。
            Time: 15 * time.Second,

            // Timeout: 发送 Ping 后，如果 3 秒内没收到客户端的响应，判定连接断开并尝试重连。
            Timeout: 3 * time.Second,

            // PermitWithoutStream: 即使当前没有任何活跃的 RPC 请求（完全空闲），也允许发送 Ping 维持连接。
            // 配合客户端侧的长连接策略，防止连接被中间网络设备或防火墙悄悄回收。
            PermitWithoutStream: true,
        }
        o.grpcDialOptions = []grpc.DialOption{
            grpc.WithTransportCredentials(insecure.NewCredentials()),
            grpc.WithKeepaliveParams(kp),
        }
    }
    if o.nodeName == "" {
        return nil, gerror.Newf("NewDiscovery NodeName %s is empty", o.nodeName)
    }
    if o.etcdClient == nil {
        return nil, gerror.Newf("NewDiscovery NodeName %s etcd client is nil", o.nodeName)
    }
    if o.redisClient == nil {
        return nil, gerror.Newf("NewDiscovery NodeName %s redis client is nil", o.nodeName)
    }
    ctx, cancel := context.WithCancel(o.ctx)
    discovery := &Discovery[T]{
        opts:             o,
        ctx:              ctx,
        cancel:           cancel,
        minLoadNodeCache: gcache.New(50).LRU().Build(),
    }
    return discovery, nil
}

// newGrpcClient 创建 gRPC 客户端连接
func (x *Discovery[T]) newGrpcClient(nc INodeConfig) (*grpc.ClientConn, error) {
    grpcClient, err := grpc.NewClient(nc.GetGRPCAddr(), x.opts.grpcDialOptions...)
    if err != nil {
        return nil, gerror.Wrapf(err, "Discovery newGrpcClient NodeName %s grpc.NewClient Addr %s dial failed", x.opts.nodeName, nc.GetGRPCAddr())
    }
    return grpcClient, nil
}

// IDNode 获取 ID 对应的节点配置
func (x *Discovery[T]) IDNode(idName string, id T) (INodeConfig, bool) {
    nodeID, err := x.opts.redisClient.Get(x.ctx, fmt.Sprintf(IDMappingKey, x.opts.project, x.opts.nodeName, idName, id)).Result()
    if err != nil {
        if !errors.Is(err, redis.Nil) {
            glog.Error(gerror.Wrapf(err, "Discovery IDNode NodeName %s IdType %s ID %v failed", x.opts.nodeName, idName, id))
        }
        return nil, false
    }
    nc, ok := x.ncMap.Load(nodeID)
    if !ok {
        glog.Warn(gerror.Newf("Discovery IDNode NodeName %s IdType %s ID %v not found", x.opts.nodeName, idName, id))
        return nil, false
    }
    return nc.(*nodeConfig), ok
}

// minLoadNode 获取负载最小的节点
func (x *Discovery[T]) minLoadNode() (*nodeConfig, bool) {
    if cached, err := x.minLoadNodeCache.Get(MinLoadNodeCacheKey); err == nil {
        return cached.(*nodeConfig), true
    }

    // 从 Redis 获取最小的节点负载
    loadKey := fmt.Sprintf(ServiceLoadKey, x.opts.project, x.opts.nodeName)
    loadMap, err := x.opts.redisClient.HGetAll(x.ctx, loadKey).Result()
    if err != nil {
        glog.Error(gerror.Wrapf(err, "Discovery minLoadNode NodeName %s HGetAll %s failed", x.opts.nodeName, loadKey))
        return nil, false
    }

    var (
        minLoad   int64
        minNodeID string
    )

    for nodeID, loadStr := range loadMap {
        if _, exists := x.ncMap.Load(nodeID); !exists {
            continue // 跳过已下线节点（Redis 有记录但 etcd 已删除）
        }
        load := cast.ToInt64(loadStr)
        if minNodeID == "" || load < minLoad {
            minLoad = load
            minNodeID = nodeID
        }
    }
    if minNodeID == "" {
        glog.Warn(gerror.Newf("Discovery minLoadNode NodeName %s no available nodes", x.opts.nodeName))
        return nil, false
    }

    nc, ok := x.ncMap.Load(minNodeID)
    if !ok {
        glog.Warn(gerror.Newf("Discovery minLoadNode NodeName %s NodeID %s not found", x.opts.nodeName, minNodeID))
        return nil, false
    }
    minLoadNode := nc.(*nodeConfig)
    _ = x.minLoadNodeCache.SetWithExpire(MinLoadNodeCacheKey, minLoadNode, x.opts.minLoadNodeTTL)

    return minLoadNode, true
}

// MinLoadNode 获取负载最小的节点
func (x *Discovery[T]) MinLoadNode() (INodeConfig, bool) {
    return x.minLoadNode()
}

// GrpcClient 获取 ID 对应的 gRPC 客户端连接
func (x *Discovery[T]) GrpcClient(idName string, id T) (*grpc.ClientConn, error) {
    var node INodeConfig
    idNode, ok := x.IDNode(idName, id)
    if ok {
        node = idNode
    } else {
        minLoadNode, ok1 := x.minLoadNode()
        if !ok1 {
            return nil, gerror.Newf("Discovery GrpcClient NodeName %s no available nodes", x.opts.nodeName)
        }
        node = minLoadNode
    }

    conn, err, _ := x.sf.Do(node.GetID(), func() (any, error) {
        if c, ok1 := x.nGrpcMap.Load(node.GetID()); ok1 {
            return c, nil
        }
        c, err := x.newGrpcClient(node)
        if err != nil {
            return nil, err
        }
        x.nGrpcMap.Store(node.GetID(), c)
        return c, nil
    })
    if err != nil {
        return nil, err
    }

    return conn.(*grpc.ClientConn), nil
}

// Start 启动发现服务
func (x *Discovery[T]) Start(_ context.Context) error {
    glog.Infof("[DISCOVERY] node %s server starting", x.opts.nodeName)
    // 初始化所有服务节点和节点 gRPC 客户端连接映射
    resp, err := x.opts.etcdClient.Get(x.ctx, fmt.Sprintf(ServiceKeyPrefix, x.opts.project, x.opts.nodeName), clientv3.WithPrefix())
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
    var wg sync.WaitGroup
    // 协程监听服务节点
    wg.Go(func() {
        x.watchServiceNode(resp.Header.Revision + 1)
    })
    wg.Wait()
    return nil
}

// Stop 停止发现服务
func (x *Discovery[T]) Stop(_ context.Context) error {
    glog.Infof("[DISCOVERY] Stop node %s server stopping", x.opts.nodeName)
    defer x.cancel()

    var grpcConnList []*grpc.ClientConn
    x.nGrpcMap.Range(func(_, value any) bool {
        if conn, ok := value.(*grpc.ClientConn); ok {
            grpcConnList = append(grpcConnList, conn)
        }
        return true
    })
    x.nGrpcMap.Clear()
    x.ncMap.Clear()
    for _, conn := range grpcConnList {
        _ = conn.Close()
    }

    return nil
}

// watchServiceNode 监听服务节点
func (x *Discovery[T]) watchServiceNode(revision int64) {
    watchChan := x.opts.etcdClient.Watch(x.ctx, fmt.Sprintf(ServiceKeyPrefix, x.opts.project, x.opts.nodeName), clientv3.WithPrefix(), clientv3.WithRev(revision))
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
                    nc := &nodeConfig{}
                    err := json.Unmarshal(event.Kv.Value, nc)
                    if err != nil {
                        glog.Error(gerror.Wrapf(err, "Discovery watchServiceNode EventTypePut NodeName %s Key %s Value %s Unmarshal etcd failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                        continue
                    }
                    x.ncMap.Store(nc.GetID(), nc)
                    if grpcClient, ok1 := x.nGrpcMap.LoadAndDelete(nc.GetID()); ok1 {
                        if conn, ok2 := grpcClient.(*grpc.ClientConn); ok2 {
                            _ = conn.Close()
                        }
                    }
                    grpcClient, err := x.newGrpcClient(nc)
                    if err != nil {
                        glog.Error(gerror.Wrapf(err, "Discovery watchServiceNode EventTypePut NodeName %s Key %s Value %s newGrpcClient failed", x.opts.nodeName, event.Kv.Key, event.Kv.Value))
                        continue
                    }
                    x.nGrpcMap.Store(nc.GetID(), grpcClient)
                case clientv3.EventTypeDelete:
                    glog.Debugf("Discovery watchServiceNode EventTypeDelete NodeName %s Key %s deleted", x.opts.nodeName, event.Kv.Key)
                    // 处理键被删除的逻辑
                    nodeID, ok := lo.Last(strings.Split(string(event.Kv.Key), "/"))
                    if !ok {
                        glog.Error(gerror.Newf("Discovery watchServiceNode EventTypeDelete NodeName %s Key %s Last Split failed", x.opts.nodeName, event.Kv.Key))
                        continue
                    }
                    x.ncMap.Delete(nodeID)
                    if grpcClient, ok1 := x.nGrpcMap.LoadAndDelete(nodeID); ok1 {
                        if conn, ok2 := grpcClient.(*grpc.ClientConn); ok2 {
                            _ = conn.Close()
                        }
                    }
                    if cached, err := x.minLoadNodeCache.Get(MinLoadNodeCacheKey); err == nil {
                        if nc, ok1 := cached.(*nodeConfig); ok1 && nc.GetID() == nodeID {
                            x.minLoadNodeCache.Remove(MinLoadNodeCacheKey)
                        }
                    }
                }
            }
        }
    }
}
