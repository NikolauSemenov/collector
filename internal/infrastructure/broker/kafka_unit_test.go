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
	commitErr    error

	messages []*kafka.Message

	subscribedTopics []string
	closed           bool
	readCalls        int
	commitCalls      int
	lastCommitted    *kafka.Message
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

func (s *stubKafkaConsumer) CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error) {
	s.commitCalls++
	s.lastCommitted = m
	return nil, s.commitErr
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

func TestConsumerKafka_Commit_NoLastMessage_IsNoop(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	stub := &stubKafkaConsumer{}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if err := c.Commit(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if stub.commitCalls != 0 {
		t.Fatalf("expected no commits when no message was read, got %d", stub.commitCalls)
	}
}

func TestConsumerKafka_Commit_AfterRead_CommitsLastMessage(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	m1 := &kafka.Message{Value: []byte("m1")}
	m2 := &kafka.Message{Value: []byte("m2")}

	stub := &stubKafkaConsumer{
		messages: []*kafka.Message{m1, m2},
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
	if string(got) != "m1" {
		t.Fatalf("expected %q, got %q", "m1", string(got))
	}

	if err := c.Commit(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if stub.commitCalls != 1 {
		t.Fatalf("expected 1 commit call, got %d", stub.commitCalls)
	}
	if stub.lastCommitted != m1 {
		t.Fatalf("expected lastCommitted to be first message")
	}

	got, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if string(got) != "m2" {
		t.Fatalf("expected %q, got %q", "m2", string(got))
	}

	if err := c.Commit(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if stub.commitCalls != 2 {
		t.Fatalf("expected 2 commit calls, got %d", stub.commitCalls)
	}
	if stub.lastCommitted != m2 {
		t.Fatalf("expected lastCommitted to be second message")
	}
}

func TestConsumerKafka_Commit_PropagatesCommitError(t *testing.T) {
	t.Parallel()

	cfg := baseTestConfig()

	wantErr := errors.New("commit failed")
	m := &kafka.Message{Value: []byte("m")}

	stub := &stubKafkaConsumer{
		messages:  []*kafka.Message{m},
		commitErr: wantErr,
	}
	spy := &factorySpy{consumer: stub}

	c, err := newConsumerKafkaWithFactory(cfg, spy.factory, func(error) bool { return false })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	_, err = c.ReadMessage()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	err = c.Commit()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
}
