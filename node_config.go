package sdn

type NodeConfig struct {
    ID      string `json:"id"`
    Name    string `json:"name"`
    Version string `json:"version"`
    Addr    string `json:"addr"`
    Load    int    `json:"load"`
}
