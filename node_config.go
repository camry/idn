package idn

type nodeConfig struct {
    ID          string `json:"id"`           // 节点ID
    Name        string `json:"name"`         // 节点名称
    Version     string `json:"version"`      // 节点版本
    BuildTime   string `json:"build_time"`   // 节点构建时间
    BuildCommit string `json:"build_commit"` // 节点构建提交
    HTTPAddr    string `json:"http_addr"`    // 节点 HTTP 地址
    GRPCAddr    string `json:"grpc_addr"`    // 节点 GRPC 地址
    TCPAddr     string `json:"tcp_addr"`     // 节点 TCP 地址
}

func (x *nodeConfig) GetID() string {
    return x.ID
}

func (x *nodeConfig) GetName() string {
    return x.Name
}

func (x *nodeConfig) GetVersion() string {
    return x.Version
}

func (x *nodeConfig) GetBuildTime() string {
    return x.BuildTime
}

func (x *nodeConfig) GetBuildCommit() string {
    return x.BuildCommit
}

func (x *nodeConfig) GetHTTPAddr() string {
    return x.HTTPAddr
}

func (x *nodeConfig) GetGRPCAddr() string {
    return x.GRPCAddr
}

func (x *nodeConfig) GetTCPAddr() string {
    return x.TCPAddr
}
