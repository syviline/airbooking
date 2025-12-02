package service

import (
	"context"
	"time"

	"github.com/Domenick1991/airbooking/internal/cache"
	"github.com/Domenick1991/airbooking/internal/domain"
	"github.com/Domenick1991/airbooking/internal/repository"
)

type FlightService struct {
	repo  *repository.FlightRepository
	cache *cache.RedisCache
	cacheTTL time.Duration
}

func NewFlightService(repo *repository.FlightRepository, cache *cache.RedisCache, cacheTTL time.Duration) *FlightService {
	return &FlightService{repo: repo, cache: cache, cacheTTL: cacheTTL}
}

func (s *FlightService) List(ctx context.Context) ([]domain.Flight, error) {
	if s.cache != nil {
		if cached, err := s.cache.GetFlights(ctx); err == nil && cached != nil {
			return cached, nil
		}
	}

	flights, err := s.repo.List(ctx)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		_ = s.cache.SetFlights(ctx, flights)
	}
	return flights, nil
}

func (s *FlightService) GetByID(ctx context.Context, id int64) (*domain.Flight, error) {
	return s.repo.GetByID(ctx, id)
}
