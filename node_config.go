package idn

import "sync/atomic"

type nodeConfig struct {
    ID          string       `json:"id"`
    Name        string       `json:"name"`
    Version     string       `json:"version"`
    BuildTime   string       `json:"build_time"`
    BuildCommit string       `json:"build_commit"`
    HTTPAddr    string       `json:"http_addr"`
    GRPCAddr    string       `json:"grpc_addr"`
    TCPAddr     string       `json:"tcp_addr"`
    Load        atomic.Int32 `json:"-"`
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

func (x *nodeConfig) GetLoad() int32 {
    return x.Load.Load()
}
