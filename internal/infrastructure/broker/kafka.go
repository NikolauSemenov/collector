package broker

import (
	"collector/internal/config"
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ConsumerKafka struct {
	config   *config.Config
	consumer *kafka.Consumer
}

func NewConsumerKafka(config *config.Config) (*ConsumerKafka, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.Servers,
		"group.id":          config.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}
	err = c.SubscribeTopics([]string{config.TopicEmail}, nil)
	if err != nil {
		return nil, err
	}
	return &ConsumerKafka{
		config:   config,
		consumer: c,
	}, nil
}

func (k *ConsumerKafka) ReadMessage() ([]byte, error) {
	msg, err := k.consumer.ReadMessage(k.config.Timeout)
	if err != nil {
		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.IsTimeout() {
			return nil, nil
		}
		return nil, err
	}
	return msg.Value, nil
}

func (k *ConsumerKafka) Close() error {
	return k.consumer.Close()
}
