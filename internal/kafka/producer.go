package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type BookingEvent struct {
	Type       string    `json:"type"`
	Token      string    `json:"token"`
	FlightID   int64     `json:"flight_id"`
	SeatNumber int       `json:"seat_number"`
	Email      string    `json:"email"`
	Status     string    `json:"status"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type Producer struct {
	brokers []string
}

func NewProducer(brokers []string) *Producer {
	return &Producer{brokers: brokers}
}

func (p *Producer) Publish(ctx context.Context, topic, key string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	writer := &kafka.Writer{Addr: kafka.TCP(p.brokers...), Topic: topic, Balancer: &kafka.LeastBytes{}}
	defer writer.Close()

	return writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: data})
}
