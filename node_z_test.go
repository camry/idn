package sdn_test

import (
    "context"
    "testing"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    "github.com/camry/g/v2/glog"
    clientv3 "go.etcd.io/etcd/client/v3"

    "gitea.meigushi.go/meigushi/sdn"
)

func defaultEtcdClient() (*clientv3.Client, error) {
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"192.168.110.5:2379"},
        DialTimeout: 5 * time.Second,
    })
    return cli, err
}

func TestNewNode(t *testing.T) {
    etcdClient, err := defaultEtcdClient()
    if err != nil {
        t.Fatal(err)
    }

    nsList := make([]server.Server, 0, 5)
    for i := 0; i < 5; i++ {
        ns := sdn.NewNode(
            sdn.Name("Guild"),
            sdn.Version("1.0.0"),
            sdn.Addr("127.0.0.1:8080"),
            sdn.EtcdClient(etcdClient),
        )
        nsList = append(nsList, ns)
    }

    app := dove.New(
        dove.Name("dove"),
        dove.Version(dove.Release),
        dove.Context(context.Background()),
        dove.Server(nsList...),
        dove.BeforeStart(func(_ context.Context) error {
            glog.Debug("BeforeStart...")
            return nil
        }),
        dove.BeforeStop(func(_ context.Context) error {
            glog.Debug("BeforeStop...")
            return nil
        }),
        dove.AfterStart(func(_ context.Context) error {
            glog.Debug("AfterStart...")
            return nil
        }),
        dove.AfterStop(func(_ context.Context) error {
            glog.Debug("AfterStop...")
            return nil
        }),
    )
    time.AfterFunc(time.Minute, func() {
        _ = app.Stop()
    })
    if err := app.Run(); err != nil {
        glog.Fatal(err)
    }
}
