package idn_test

import (
    "context"
    "sync"
    "testing"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    "github.com/camry/g/v2/glog"
    "github.com/stretchr/testify/assert"

    "gitea.meigushi.go/meigushi/idn"
)

func TestNewDiscovery(t *testing.T) {
    etcdClient, err := defaultEtcdClient()
    if err != nil {
        t.Fatal(err)
    }

    servers := make([]server.Server, 0, 5)
    dsList := make([]*idn.Discovery[int32], 0, 5)
    for i := 0; i < 1; i++ {
        ds, err1 := idn.NewDiscovery[int32](
            idn.DiscoveryNodeName("guild"),
            idn.DiscoveryEtcdClient(etcdClient),
        )
        assert.NoError(t, err1)
        servers = append(servers, ds)
        dsList = append(dsList, ds)
    }

    time.AfterFunc(time.Second*1, func() {
        var wg sync.WaitGroup
        for i := 0; i < 3; i++ {
            wg.Go(func() {
                _, err = dsList[0].GrpcClient(int32(i))
                if err != nil {
                    t.Fatal(err)
                }
            })
        }
        wg.Wait()
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
