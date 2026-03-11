package idn

const (
    MutexPrefix             = "/idn/nodes/%s/mutex"      // 节点互斥锁键
    DiscoveryMappingsPrefix = "/idn/nodes/%s/mapping"    // 发现ID映射键
    IdMappingKey            = "/idn/nodes/%s/mapping/%v" // 节点ID映射键
    DiscoveryServicesPrefix = "/idn/nodes/%s/service"    // 发现服务键
    ServiceKey              = "/idn/nodes/%s/service/%s" // 节点服务键
)

type IDType interface {
    int32 | int64 | uint32 | uint64 | string
}
