package idn_test

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    "github.com/camry/g/v2/glog"
    "github.com/camry/g/v2/gnet/gipv4"
    "github.com/stretchr/testify/assert"
    clientv3 "go.etcd.io/etcd/client/v3"

    "gitea.meigushi.go/meigushi/idn"
)

func defaultEtcdClient() (*clientv3.Client, error) {
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"192.168.110.5:2379"},
        Username:    "root",
        Password:    "123698745",
        DialTimeout: 5 * time.Second,
    })
    return cli, err
}

func TestNewNode(t *testing.T) {
    etcdClient, err := defaultEtcdClient()
    if err != nil {
        t.Fatal(err)
    }
    intranetIp, err := gipv4.GetIntranetIp()
    if err != nil {
        t.Fatal(err)
    }

    servers := make([]server.Server, 0, 5)
    for i := 1; i <= 5; i++ {
        ns, err1 := idn.NewNode[int32](
            idn.NodeName("guild"),
            idn.NodeVersion("1.0.0"),
            idn.NodeBuildTime("2026-04-23 17:30:00"),
            idn.NodeBuildCommit("Git Latest Commit Hash"),
            idn.NodeGRPCAddr(fmt.Sprintf("%s:%d", intranetIp, 6000+i)),
            idn.NodeEtcdClient(etcdClient),
        )
        assert.NoError(t, err1)
        ns.SetCallRemoveIDFunc(func(id int32) error {
            glog.Debugf("remove id %d", id)
            return nil
        })
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
    time.AfterFunc(time.Second*20, func() {
        _ = app.Stop()
    })
    if err := app.Run(); err != nil {
        glog.Fatal(err)
    }
}
