package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Domenick1991/airbooking/config"
	"github.com/Domenick1991/airbooking/internal/bootstrap"
	"github.com/Domenick1991/airbooking/internal/cache"
	"github.com/Domenick1991/airbooking/internal/kafka"
	"github.com/Domenick1991/airbooking/internal/repository"
	"github.com/Domenick1991/airbooking/internal/service"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	if err := bootstrap.Run(ctx, cfg, flightService, bookingService); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
