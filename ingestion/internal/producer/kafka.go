package producer

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	messagesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_messages_sent_total",
		Help: "Total number of messages successfully sent to Kafka",
	}, []string{"topic"})

	sendErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_send_errors_total",
		Help: "Total number of errors encountered while sending to Kafka",
	}, []string{"topic"})

	sendDurationMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_send_duration_ms",
		Help:    "Latency of Kafka publish operations in milliseconds",
		Buckets: []float64{1, 5, 10, 50, 100, 500},
	})
)

type Producer struct {
	syncProducer sarama.SyncProducer
}

func New(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Producer.Retry.Max = 5
	config.Net.MaxOpenRequests = 1
	config.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &Producer{
		syncProducer: p,
	}, nil
}

func (p *Producer) Publish(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	start := time.Now()
	_, _, err := p.syncProducer.SendMessage(msg)
	duration := float64(time.Since(start).Milliseconds())

	sendDurationMs.Observe(duration)

	if err != nil {
		sendErrorsTotal.WithLabelValues(topic).Inc()
		return err
	}

	messagesSentTotal.WithLabelValues(topic).Inc()
	return nil
}

func (p *Producer) Close() error {
	return p.syncProducer.Close()
}
