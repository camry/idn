package sdn

import "google.golang.org/grpc"

type idConfig struct {
    nodeConfig *nodeConfig
    grpcClient *grpc.ClientConn
}
