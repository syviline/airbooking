package bootstrap

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Domenick1991/airbooking/config"
	bookingsapi "github.com/Domenick1991/airbooking/internal/api/bookings_service_api"
	flightsapi "github.com/Domenick1991/airbooking/internal/api/flights_service_api"
	"github.com/Domenick1991/airbooking/internal/pb/bookings_api"
	"github.com/Domenick1991/airbooking/internal/pb/flights_api"
	"github.com/Domenick1991/airbooking/internal/service"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Servers struct {
	grpcServer *grpc.Server
	httpServer *http.Server
}

// Run starts gRPC and HTTP (grpc-gateway + swagger) servers and blocks until context is canceled or a server fails.
func Run(ctx context.Context, cfg *config.Config, flightSvc *service.FlightService, bookingSvc *service.BookingService) error {
	s, err := newServers(cfg, flightSvc, bookingSvc)
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)

	// gRPC server
	lis, err := net.Listen("tcp", cfg.GRPC.Address)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", cfg.GRPC.Address, err)
	}
	go func() { errCh <- s.grpcServer.Serve(lis) }()

	// HTTP gateway + swagger
	go func() { errCh <- s.httpServer.ListenAndServe() }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.grpcServer.GracefulStop()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown http server: %w", err)
		}
		return nil
	}
}

func newServers(cfg *config.Config, flightSvc *service.FlightService, bookingSvc *service.BookingService) (*Servers, error) {
	grpcSrv := grpc.NewServer()

	flightsServer := flightsapi.NewServer(flightSvc)
	bookingsServer := bookingsapi.NewServer(bookingSvc)

	flights_api.RegisterFlightsServiceServer(grpcSrv, flightsServer)
	bookings_api.RegisterBookingsServiceServer(grpcSrv, bookingsServer)

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := flights_api.RegisterFlightsServiceHandlerFromEndpoint(context.Background(), mux, cfg.GRPC.Address, opts); err != nil {
		return nil, fmt.Errorf("register flights gateway: %w", err)
	}
	if err := bookings_api.RegisterBookingsServiceHandlerFromEndpoint(context.Background(), mux, cfg.GRPC.Address, opts); err != nil {
		return nil, fmt.Errorf("register bookings gateway: %w", err)
	}

	handler := http.NewServeMux()
	handler.Handle("/", mux)

	if cfg.HTTP.SwaggerDir != "" {
		fs := http.FileServer(http.Dir(cfg.HTTP.SwaggerDir))
		handler.Handle("/swagger/", http.StripPrefix("/swagger/", fs))
	}

	httpSrv := &http.Server{
		Addr:    cfg.HTTP.Address,
		Handler: handler,
	}

	return &Servers{
		grpcServer: grpcSrv,
		httpServer: httpSrv,
	}, nil
}
