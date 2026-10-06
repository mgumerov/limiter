// Package proto contains the gRPC API of the limiter and the code generated from it.
package proto

//go:generate protoc -I .. --go_out=.. --go_opt=paths=source_relative --go-grpc_out=.. --go-grpc_opt=paths=source_relative proto/limiter.proto
