package idn_test

import (
    "context"
    "testing"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    "github.com/camry/g/v2/glog"

    "gitea.meigushi.go/meigushi/idn"
)

func TestNewDiscovery(t *testing.T) {
    etcdClient, err := defaultEtcdClient()
    if err != nil {
        t.Fatal(err)
    }

    servers := make([]server.Server, 0, 5)
    dsList := make([]*idn.Discovery[int32], 0, 5)
    for i := 0; i < 5; i++ {
        ds := idn.NewDiscovery[int32](
            idn.DiscoveryNodeName("guild"),
            idn.DiscoveryEtcdClient(etcdClient),
        )
        servers = append(servers, ds)
        dsList = append(dsList, ds)
    }

    time.AfterFunc(time.Second*1, func() {
        for i := 0; i < 3; i++ {
            _, err = dsList[0].GrpcClient(int32(i))
            if err != nil {
                t.Fatal(err)

            }
            time.Sleep(time.Second * 3)
        }
    })

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
    time.AfterFunc(time.Second*10, func() {
        _ = app.Stop()
    })
    if err := app.Run(); err != nil {
        glog.Fatal(err)
    }
}
