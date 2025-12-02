package service

import (
	"context"
	"errors"
	"time"

	"github.com/Domenick1991/airbooking/internal/cache"
	"github.com/Domenick1991/airbooking/internal/domain"
	"github.com/Domenick1991/airbooking/internal/kafka"
	"github.com/Domenick1991/airbooking/internal/repository"
	"github.com/google/uuid"
)

type BookingService struct {
	bookings            *repository.BookingRepository
	flights             *repository.FlightRepository
	cache               *cache.RedisCache
	producer            *kafka.Producer
	bookingTopic        string
	notificationsTopic  string
	holdTTL             time.Duration
	confirmationTTL     time.Duration
}

type CreateBookingInput struct {
	FlightID   int64  `json:"flight_id"`
	SeatNumber int    `json:"seat_number"`
	Email      string `json:"email"`
}

type BookingServiceOption func(*BookingService)

func WithNotificationsTopic(topic string) BookingServiceOption {
	return func(s *BookingService) {
		s.notificationsTopic = topic
	}
}

func NewBookingService(bookings *repository.BookingRepository, flights *repository.FlightRepository, cache *cache.RedisCache, producer *kafka.Producer, bookingTopic string, holdTTL, confirmationTTL time.Duration, opts ...BookingServiceOption) *BookingService {
	service := &BookingService{
		bookings:        bookings,
		flights:         flights,
		cache:           cache,
		producer:        producer,
		bookingTopic:    bookingTopic,
		holdTTL:         holdTTL,
		confirmationTTL: confirmationTTL,
	}
	for _, opt := range opts {
		opt(service)
	}
	return service
}

func (s *BookingService) CreateBooking(ctx context.Context, input CreateBookingInput) (*domain.Booking, error) {
	if input.SeatNumber <= 0 {
		return nil, errors.New("seat number must be positive")
	}
	if input.Email == "" {
		return nil, errors.New("email is required")
	}

	locked := false
	if s.cache != nil {
		ok, err := s.cache.AcquireSeatLock(ctx, input.FlightID, input.SeatNumber, s.holdTTL)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("seat is already locked")
		}
		locked = true
		defer s.cache.ReleaseSeatLock(ctx, input.FlightID, input.SeatNumber)
	}

	expiresIn := s.confirmationTTL
	if expiresIn == 0 {
		expiresIn = s.holdTTL
	}

	booking := &domain.Booking{
		FlightID:   input.FlightID,
		SeatNumber: input.SeatNumber,
		Token:      uuid.NewString(),
		ExpiresAt:  time.Now().Add(expiresIn),
		Email:      input.Email,
	}

	if err := s.bookings.CreatePending(ctx, booking); err != nil {
		if locked {
			_ = s.cache.ReleaseSeatLock(ctx, input.FlightID, input.SeatNumber)
		}
		return nil, err
	}

	booking.Status = domain.BookingStatusPending
	_ = s.publish(ctx, "booking_created", booking)
	return booking, nil
}

func (s *BookingService) ConfirmBooking(ctx context.Context, token string) (*domain.Booking, error) {
	current, err := s.bookings.GetByToken(ctx, token)
	if err != nil {
		return nil, err
	}
	if current.Status != domain.BookingStatusPending {
		return nil, errors.New("booking is not pending")
	}

	updated, err := s.bookings.UpdateStatus(ctx, token, domain.BookingStatusConfirmed)
	if err != nil {
		return nil, err
	}
	_ = s.publish(ctx, "booking_confirmed", updated)
	if s.cache != nil {
		_ = s.cache.ReleaseSeatLock(ctx, updated.FlightID, updated.SeatNumber)
	}
	return updated, nil
}

func (s *BookingService) CancelBooking(ctx context.Context, token string) (*domain.Booking, error) {
	current, err := s.bookings.GetByToken(ctx, token)
	if err != nil {
		return nil, err
	}
	if current.Status == domain.BookingStatusCancelled || current.Status == domain.BookingStatusExpired {
		return current, nil
	}

	updated, err := s.bookings.UpdateStatus(ctx, token, domain.BookingStatusCancelled)
	if err != nil {
		return nil, err
	}
	_ = s.bookings.ReleaseSeat(ctx, updated.FlightID)
	_ = s.publish(ctx, "booking_cancelled", updated)
	if s.cache != nil {
		_ = s.cache.ReleaseSeatLock(ctx, updated.FlightID, updated.SeatNumber)
	}
	return updated, nil
}

func (s *BookingService) ExpirePendingBookings(ctx context.Context) ([]domain.Booking, error) {
	deadline := time.Now()
	expired, err := s.bookings.ExpirePendingBefore(ctx, deadline)
	if err != nil {
		return nil, err
	}
	for _, b := range expired {
		_ = s.bookings.ReleaseSeat(ctx, b.FlightID)
		_ = s.publish(ctx, "booking_expired", &b)
		if s.cache != nil {
			_ = s.cache.ReleaseSeatLock(ctx, b.FlightID, b.SeatNumber)
		}
	}
	return expired, nil
}

func (s *BookingService) publish(ctx context.Context, eventType string, booking *domain.Booking) error {
	if s.producer == nil || s.bookingTopic == "" {
		return nil
	}
	event := kafka.BookingEvent{
		Type:       eventType,
		Token:      booking.Token,
		FlightID:   booking.FlightID,
		SeatNumber: booking.SeatNumber,
		Email:      booking.Email,
		Status:     string(booking.Status),
		ExpiresAt:  booking.ExpiresAt,
	}
	if err := s.producer.Publish(ctx, s.bookingTopic, booking.Token, event); err != nil {
		return err
	}
	if s.notificationsTopic != "" {
		return s.producer.Publish(ctx, s.notificationsTopic, booking.Token, event)
	}
	return nil
}
