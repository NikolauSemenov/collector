package broker

import (
	"collector/internal/config"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type stubKafkaConsumer struct {
	subscribeErr error
	readErr      error
	closeErr     error

	messages []*kafka.Message

	subscribedTopics []string
	closed           bool
	readCalls        int
}

func (s *stubKafkaConsumer) SubscribeTopics(topics []string, _ kafka.RebalanceCb) error {
	s.subscribedTopics = append([]string(nil), topics...)
	return s.subscribeErr
}

func (s *stubKafkaConsumer) ReadMessage(_ time.Duration) (*kafka.Message, error) {
	s.readCalls++
	if s.readErr != nil {
		return nil, s.readErr
	}
	if len(s.messages) == 0 {
		return nil, nil
	}
	m := s.messages[0]
	s.messages = s.messages[1:]
	return m, nil
}

func (s *stubKafkaConsumer) Close() error {
	s.closed = true
	return s.closeErr
}

type factorySpy struct {
	consumer *stubKafkaConsumer
	err      error

	calls int
	last  *kafka.ConfigMap
}

func (f *factorySpy) factory(cm *kafka.ConfigMap) (kafkaConsumerAPI, error) {
	f.calls++
	f.last = cm
	if f.err != nil {
		return nil, f.err
	}
	return f.consumer, nil
}

func baseTestConfig() *config.Config {
	return &config.Config{
		BrokerConfig: config.BrokerConfig{
			Servers:       "localhost:29092",
			TopicEmail:    "verify_email_events",
			ConsumerGroup: "email.smtp",
			Timeout:       10 * time.Millisecond,
		},
		EmailConfig: config.EmailConfig{
			EmailHost:      "smtp.example.com",
			EmailPort:      465,
			EmailUsername:  "user@example.com",
			EmailPassword:  "app-password",
			EmailSecretKey: "not-used-here",
		},
	}
}

func TestNewConsumerKafkaWithFactory_NilConfig(t *testing.T) {
	t.Parallel()

	spy := &factorySpy{consumer: &stubKafkaConsumer{}}
	_, err := newConsumerKafkaWithFactory(nil, spy.factory, func(error) bool { return false })
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestNewConsumerKafkaWithFactory_NilFactory(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()
	_, err := newConsumerKafkaWithFactory(cfg, nil, func(error) bool { return false })
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestNewConsumerKafkaWithFactory_NilIsTimeout(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()
	spy := &factorySpy{consumer: &stubKafkaConsumer{}}

	_, err := newConsumerKafkaWithFactory(cfg, spy.factory, nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestNewConsumerKafkaWithFactory_FactoryError(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()
	spy := &factorySpy{
		consumer: &stubKafkaConsumer{},
		err:      errors.New("factory failed"),
	}

	_, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if spy.calls != 1 {
		t.Fatalf("expected factory to be called once, got %d", spy.calls)
	}
}

func TestNewConsumerKafkaWithFactory_SubscribesToConfiguredTopic(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	stub := &stubKafkaConsumer{}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if c == nil {
		t.Fatalf("expected non-nil consumer")
	}

	if len(stub.subscribedTopics) != 1 || stub.subscribedTopics[0] != cfg.TopicEmail {
		t.Fatalf("expected subscribedTopics=[%q], got %v", cfg.TopicEmail, stub.subscribedTopics)
	}
}

func TestNewConsumerKafkaWithFactory_SubscribeError_ClosesConsumer(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	stub := &stubKafkaConsumer{subscribeErr: errors.New("subscribe failed")}
	spy := &factorySpy{consumer: stub}

	_, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !stub.closed {
		t.Fatalf("expected consumer to be closed on subscribe error")
	}
}

func TestConsumerKafka_ReadMessage_TimeoutReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	timeoutErr := errors.New("timeout")
	stub := &stubKafkaConsumer{readErr: timeoutErr}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(err error) bool { return err == timeoutErr })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	msg, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if msg != nil {
		t.Fatalf("expected nil msg on timeout, got %v", msg)
	}
}

func TestConsumerKafka_ReadMessage_NonTimeoutPropagatesError(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	readErr := errors.New("read failed")
	stub := &stubKafkaConsumer{readErr: readErr}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	msg, err := c.ReadMessage()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, readErr) {
		t.Fatalf("expected error %v, got %v", readErr, err)
	}
	if msg != nil {
		t.Fatalf("expected nil msg on error, got %v", msg)
	}
}

func TestConsumerKafka_ReadMessage_EmptyValueReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	stub := &stubKafkaConsumer{
		messages: []*kafka.Message{
			{Value: []byte{}},
		},
	}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	msg, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if msg != nil {
		t.Fatalf("expected nil msg for empty value, got %v", msg)
	}
}

func TestConsumerKafka_ReadMessage_ReturnsValue(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	want := []byte("hello")
	stub := &stubKafkaConsumer{
		messages: []*kafka.Message{
			{Value: want},
		},
	}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("expected %q, got %q", string(want), string(got))
	}
}

func TestConsumerKafka_Close_DelegatesToUnderlyingConsumer(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	stub := &stubKafkaConsumer{}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !stub.closed {
		t.Fatalf("expected underlying consumer to be closed")
	}
}
