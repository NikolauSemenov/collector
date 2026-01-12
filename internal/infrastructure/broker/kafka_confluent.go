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
		"bootstrap.servers": cfg.Servers,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
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
	return msg.Value, nil
}

func (k *ConsumerKafka) Close() error {
	return k.consumer.Close()
}
