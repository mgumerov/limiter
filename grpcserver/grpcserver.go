package grpcserver

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"google.golang.org/grpc"

	"limiter/server"
	pb "limiter/grpcserver/proto"
)

type GRPCServer struct {
	grpc *grpc.Server
}

var _ server.Server = (*GRPCServer)(nil)

func (s *GRPCServer) Shutdown() error {
	s.grpc.GracefulStop()
	return nil
}

type limiterService struct {
	pb.UnimplementedLimiterServer

	processor   server.Processor
	handlerTime *metrics.Histogram
}

func (s *limiterService) Request(
	ctx context.Context,
	req *pb.RequestRequest,
) (*pb.RequestResponse, error) {

	start := time.Now()

	slog.Debug(
		"Serving request",
		"key", req.Key,
		"amount", req.Amount,
	)

	result := s.processor.Request(
		req.Key,
		req.Amount,
	)

	s.handlerTime.UpdateDuration(start)

	return &pb.RequestResponse{
		Granted: result.Granted,
	}, nil
}

func CreateGRPCServer(
	processor server.Processor,
	grpcFailed chan<- struct{},
	port string,
	myMetrics *metrics.Set,
) server.Server {

	grpcSrv := createGRPC(
		processor,
		grpcFailed,
		myMetrics,
	)

	startGRPC(
		grpcSrv,
		grpcFailed,
		port,
	)

	return grpcSrv
}

func createGRPC(
	processor server.Processor,
	grpcFailed chan<- struct{},
	myMetrics *metrics.Set,
) *GRPCServer {

	handlerTime := myMetrics.NewHistogram("handler_time")

	grpcSrv := grpc.NewServer(
		grpc.MaxConcurrentStreams(1000),
		grpc.UnaryInterceptor(
			func(
				ctx context.Context,
				req any,
				info *grpc.UnaryServerInfo,
				handler grpc.UnaryHandler,
			) (resp any, err error) {

				defer func() {
					if r := recover(); r != nil {
						grpcFailed <- struct{}{}
						panic(r)
					}
				}()

				return handler(ctx, req)
			},
		),
	)

	pb.RegisterLimiterServer(
		grpcSrv,
		&limiterService{
			processor:   processor,
			handlerTime: handlerTime,
		},
	)

	return &GRPCServer{
		grpc: grpcSrv,
	}
}

func startGRPC(
	srv *GRPCServer,
	grpcFailed chan<- struct{},
	port string,
) {
	slog.Info("Starting GRPC server (use PORT to change listening port)", "Port", port)
	
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("gRPC server panicked", "error", r)
				grpcFailed <- struct{}{}
			}
		}()

		lis, err := net.Listen(
			"tcp",
			":" + port,
		)
		if err != nil {
			slog.Error(
				"gRPC server startup failed",
				"error",
				err,
			)
			grpcFailed <- struct{}{}
			return
		}

		if err := srv.grpc.Serve(lis); err != nil {
			slog.Error(
				"gRPC server failed",
				"error",
				err,
			)
			grpcFailed <- struct{}{}
		}
	}()
}