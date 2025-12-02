package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Domenick1991/airbooking/api"
	"github.com/Domenick1991/airbooking/config"
	"github.com/Domenick1991/airbooking/internal/cache"
	"github.com/Domenick1991/airbooking/internal/kafka"
	"github.com/Domenick1991/airbooking/internal/repository"
	"github.com/Domenick1991/airbooking/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfgPath := os.Getenv("CONFIG_PATH")
	if cfgPath == "" {
		cfgPath = "config.yaml"
	}

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.Database.DSN())
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	redisCache := cache.NewRedisCache(cfg.Redis, time.Duration(cfg.Booking.FlightsCacheTTL)*time.Second)
	producer := kafka.NewProducer(cfg.Kafka.Brokers)

	flightRepo := repository.NewFlightRepository(pool)
	bookingRepo := repository.NewBookingRepository(pool)
	flightService := service.NewFlightService(flightRepo, redisCache, time.Duration(cfg.Booking.FlightsCacheTTL)*time.Second)
	bookingService := service.NewBookingService(
		bookingRepo,
		flightRepo,
		redisCache,
		producer,
		cfg.Kafka.BookingEventsTopic,
		time.Duration(cfg.Booking.HoldTTLMinutes)*time.Minute,
		time.Duration(cfg.Booking.ConfirmationTTL)*time.Minute,
		service.WithNotificationsTopic(cfg.Kafka.NotificationsTopic),
	)

	r := gin.Default()
	apiGroup := r.Group("/api/v1")
	flightHandler := api.NewFlightHandler(flightService)
	bookingHandler := api.NewBookingHandler(bookingService)

	flightHandler.Register(apiGroup.Group("/flights"))
	bookingHandler.Register(apiGroup.Group("/bookings"))

	serverErr := make(chan error, 1)
	go func() { serverErr <- r.Run(cfg.HTTP.Address) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		log.Fatalf("server error: %v", err)
	case s := <-sig:
		log.Printf("received signal %v, shutting down", s)
	}
}
