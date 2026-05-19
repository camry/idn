package idn

const (
    MutexMappingPrefix      = "%s/idn/nodes/%s/mutex/mapping" // ID互斥锁键前缀
    DiscoveryMappingsPrefix = "%s/idn/nodes/%s/mapping"       // 发现ID映射键
    IDMappingKey            = "%s/idn/nodes/%s/mapping/%v"    // 节点ID映射键
    DiscoveryServicesPrefix = "%s/idn/nodes/%s/service"       // 发现服务键
    ServiceKey              = "%s/idn/nodes/%s/service/%s"    // 节点服务键
)

const (
    DefaultLimit = int64(1000) // 默认批量操作限制
)

type IDType interface {
    int32 | int64 | uint32 | uint64 | string
}

type INodeConfig interface {
    GetID() string
    GetName() string
    GetVersion() string
    GetBuildTime() string
    GetBuildCommit() string
    GetHTTPAddr() string
    GetGRPCAddr() string
    GetTCPAddr() string
    GetLoad() int32
}
