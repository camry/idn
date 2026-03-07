package sdn_test

import (
    "context"
    "testing"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    "github.com/camry/g/v2/glog"

    "gitea.meigushi.go/meigushi/sdn"
)

func TestNewDiscovery(t *testing.T) {
    etcdClient, err := defaultEtcdClient()
    if err != nil {
        t.Fatal(err)
    }

    servers := make([]server.Server, 0, 5)
    for i := 0; i < 5; i++ {
        ns := sdn.NewDiscovery(
            sdn.DiscoveryNodeName("Guild"),
            sdn.DiscoveryEtcdClient(etcdClient),
        )
        servers = append(servers, ns)
    }

    app := dove.New(
        dove.Name("dove"),
        dove.Version(dove.Release),
        dove.Context(context.Background()),
        dove.Server(servers...),
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
