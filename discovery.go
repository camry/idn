package sdn

import (
    "context"

    clientv3 "go.etcd.io/etcd/client/v3"
)

type Discovery struct {
    etcdClient *clientv3.Client
}

// NewDiscovery 创建发现服务
func NewDiscovery(etcdClient *clientv3.Client) *Discovery {
    return &Discovery{
        etcdClient: etcdClient,
    }
}

// Start 启动发现服务
func (x *Discovery) Start(ctx context.Context) error {
    return nil
}

// Stop 停止发现服务
func (x *Discovery) Stop(ctx context.Context) error {
    return nil
}
