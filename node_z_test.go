package idn_test

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/camry/dove/v2"
    "github.com/camry/dove/v2/server"
    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/camry/g/v2/gnet/gipv4"
    "github.com/redis/go-redis/v9"
    "github.com/stretchr/testify/assert"
    clientv3 "go.etcd.io/etcd/client/v3"

    "gitea.meigushi.go/meigushi/idn"
)

func defaultEtcdClient() (*clientv3.Client, error) {
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"192.168.11.77:2379"},
        Username:    "root",
        Password:    "e5WZPHqrHitnd2Qu",
        DialTimeout: 5 * time.Second,
    })
    return cli, err
}

func defaultRedisClient() (*redis.Client, error) {
    client := redis.NewClient(&redis.Options{
        Addr:     "192.168.11.77:6379",
        Password: "suZSTdAu8LGpQ7B4",
        DB:       0,
    })
    ping := client.Ping(context.Background())
    if ping.Err() != nil {
        return nil, gerror.Wrap(ping.Err(), "defaultRedisClient Redis Client Ping Failed")
    }
    return client, nil
}

func TestNewNode(t *testing.T) {
    etcdClient, err := defaultEtcdClient()
    if err != nil {
        t.Fatal(err)
    }
    redisClient, err := defaultRedisClient()
    if err != nil {
        t.Fatal(err)
    }
    intranetIp, err := gipv4.GetIntranetIp()
    if err != nil {
        t.Fatal(err)
    }

    servers := make([]server.Server, 0, 5)
    nodes := make([]*idn.Node[int32], 0, 5)
    for i := 1; i <= 5; i++ {
        ns, err1 := idn.NewNode[int32](
            idn.NodeProject("project"),
            idn.NodeName("server"),
            idn.NodeVersion("1.0.0"),
            idn.NodeBuildTime("2026-04-23 17:30:00"),
            idn.NodeBuildCommit("Git Latest Commit Hash"),
            idn.NodeGRPCAddr(fmt.Sprintf("%s:%d", intranetIp, 6000+i)),
            idn.NodeEtcdClient(etcdClient),
            idn.NodeRedisClient(redisClient),
            idn.NodeLeaseTTL(30),
        )
        assert.NoError(t, err1)
        servers = append(servers, ns)
        nodes = append(nodes, ns)
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
    time.AfterFunc(time.Second, func() {
        for i := 0; i < 5; i++ {
            for j := int32(1); j <= 100; j++ {
                err = nodes[i].BindID("uid", j, time.Minute)
                assert.NoError(t, err)
            }
        }
    })
    time.AfterFunc(time.Second*30, func() {
        for i := 0; i < 5; i++ {
            for j := int32(1); j <= 100; j++ {
                err = nodes[i].UnbindID("uid", j)
                assert.NoError(t, err)
            }
        }
    })
    time.AfterFunc(time.Second*35, func() {
        _ = app.Stop()
    })
    if err = app.Run(); err != nil {
        glog.Fatal(err)
    }
}
