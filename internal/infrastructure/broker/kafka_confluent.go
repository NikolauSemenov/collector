package broker

import (
	"collector/internal/config"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaConsumerAPI interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type kafkaConsumerFactory func(*kafka.ConfigMap) (kafkaConsumerAPI, error)

func defaultKafkaConsumerFactory(cfg *kafka.ConfigMap) (kafkaConsumerAPI, error) {
	return kafka.NewConsumer(cfg)
}

type ConsumerKafka struct {
	config    *config.Config
	consumer  kafkaConsumerAPI
	isTimeout func(error) bool

	lastMu  chan struct{}
	lastMsg *kafka.Message
}

func NewConsumerKafka(config *config.Config) (*ConsumerKafka, error) {
	return newConsumerKafkaWithFactory(config, defaultKafkaConsumerFactory, defaultKafkaIsTimeout)
}

func newConsumerKafkaWithFactory(
	cfg *config.Config,
	factory kafkaConsumerFactory,
	isTimeout func(error) bool,
) (*ConsumerKafka, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if factory == nil {
		return nil, fmt.Errorf("kafka consumer factory is nil")
	}
	if isTimeout == nil {
		return nil, fmt.Errorf("isTimeout function is nil")
	}

	c, err := factory(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Servers,
		"group.id":           cfg.ConsumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	if err := c.SubscribeTopics([]string{cfg.TopicEmail}, nil); err != nil {
		_ = c.Close()
		return nil, err
	}

	return &ConsumerKafka{
		config:    cfg,
		consumer:  c,
		isTimeout: isTimeout,
		lastMu:    make(chan struct{}, 1),
	}, nil
}

func defaultKafkaIsTimeout(err error) bool {
	if err == nil {
		return false
	}
	var kafkaErr kafka.Error
	return errors.As(err, &kafkaErr) && kafkaErr.IsTimeout()
}

func (k *ConsumerKafka) ReadMessage() ([]byte, error) {
	msg, err := k.consumer.ReadMessage(k.config.Timeout)
	if err != nil {
		if k.isTimeout(err) {
			return nil, nil
		}
		return nil, err
	}
	if msg == nil || len(msg.Value) == 0 {
		return nil, nil
	}

	select {
	case k.lastMu <- struct{}{}:
	default:
	}
	k.lastMsg = msg
	<-k.lastMu

	return msg.Value, nil
}

func (k *ConsumerKafka) Commit() error {
	if k == nil || k.consumer == nil {
		return fmt.Errorf("consumer is not initialized")
	}

	select {
	case k.lastMu <- struct{}{}:
	default:
	}
	msg := k.lastMsg
	<-k.lastMu

	if msg == nil {
		return nil
	}

	_, err := k.consumer.CommitMessage(msg)
	return err
}

func (k *ConsumerKafka) Close() error {
	return k.consumer.Close()
}
