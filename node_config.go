package sdn

type nodeConfig struct {
    ID      string `json:"id"`
    Name    string `json:"name"`
    Version string `json:"version"`
    Addr    string `json:"addr"`
    Load    int32  `json:"load"`
}
