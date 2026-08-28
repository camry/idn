package idn

const (
    // ServiceKey 节点服务键
    // note: 服务键格式为 /项目名称/idn/nodes/节点名称/service/节点ID
    ServiceKey = "/%s/idn/nodes/%s/service/%s"

    // ServiceKeyPrefix 服务键前缀
    // note: 服务键前缀格式为 /项目名称/idn/nodes/节点名称/service
    ServiceKeyPrefix = "/%s/idn/nodes/%s/service"

    // ServiceLoadKey 服务负载键
    // note: 服务负载键格式为 {项目名称}:idn:{节点名称}:load
    // eg: wild:idn:gs:load
    ServiceLoadKey = "%s:idn:%s:load"

    // IDMappingKey 节点ID映射键
    // note: ID映射键格式为 {项目名称}:idn:{节点名称}:{ID名称}:{ID}
    // eg: wild:idn:gs:uid:10001-12872001
    IDMappingKey = "%s:idn:%s:%s:%v"

    // MinLoadNodeCacheKey 最小负载节点缓存键
    MinLoadNodeCacheKey = "min-load-node"
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
    GetDebug() bool
}
