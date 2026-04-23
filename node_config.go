package idn

type nodeConfig struct {
    ID          string `json:"id"`
    Name        string `json:"name"`
    Version     string `json:"version"`
    BuildTime   string `json:"build_time"`
    BuildCommit string `json:"build_commit"`
    Addr        string `json:"addr"`
    Load        int32  `json:"-"`
}
