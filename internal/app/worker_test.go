package app

import (
	"collector/internal/config"
	"collector/internal/logging"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/fernet/fernet-go"
)

type fakeSender struct {
	lastEmail  string
	lastHeader string
	lastText   string
	err        error
	calls      int
}

func (s *fakeSender) Send(email, header, text string) error {
	s.calls++
	s.lastEmail = email
	s.lastHeader = header
	s.lastText = text
	return s.err
}

type fakeConsumer struct {
	msgs   [][]byte
	idx    int
	err    error
	closed bool

	commitErr error
	commits   int

	closedCh chan struct{}
}

func (c *fakeConsumer) ReadMessage() ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.idx < len(c.msgs) {
		m := c.msgs[c.idx]
		c.idx++
		return m, nil
	}

	if c.closedCh == nil {
		c.closedCh = make(chan struct{})
	}

	<-c.closedCh
	return nil, context.Canceled
}

func (c *fakeConsumer) Commit() error {
	c.commits++
	return c.commitErr
}

func (c *fakeConsumer) Close() error {
	c.closed = true
	if c.closedCh == nil {
		c.closedCh = make(chan struct{})
	}
	select {
	case <-c.closedCh:
	default:
		close(c.closedCh)
	}
	return nil
}

func newTestConfig(t *testing.T) *config.Config {
	t.Helper()

	key := fernet.Key{}
	key.Generate()

	return &config.Config{
		EmailConfig: config.EmailConfig{
			EmailSecretKey: key.Encode(),
		},
	}
}

func TestWorker_HandleMessage_OK(t *testing.T) {
	t.Parallel()

	s := &fakeSender{}
	cfg := newTestConfig(t)
	w := &Worker{
		sender: s,
		config: cfg,
	}

	key, err := fernet.DecodeKey(cfg.EmailSecretKey)
	if err != nil {
		t.Fatalf("failed to decode key: %v", err)
	}

	originalCode := "123456"
	encryptedToken, err := fernet.EncryptAndSign([]byte(originalCode), key)
	if err != nil {
		t.Fatalf("failed to encrypt code: %v", err)
	}

	data := []byte(`{"name":"Ivan","email":"user@example.com","code":"` + string(encryptedToken) + `"}`)

	if err := w.handleMessage(context.Background(), data); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if s.calls != 1 {
		t.Fatalf("expected 1 send call, got %d", s.calls)
	}
	if s.lastEmail != "user@example.com" {
		t.Fatalf("expected email user@example.com, got %q", s.lastEmail)
	}
	if strings.TrimSpace(s.lastHeader) == "" {
		t.Fatalf("expected non-empty header")
	}
	if strings.TrimSpace(s.lastText) == "" {
		t.Fatalf("expected non-empty text")
	}

	expectedBody := "Код доступа: " + originalCode
	if s.lastText != expectedBody {
		t.Fatalf("expected body %q, got %q", expectedBody, s.lastText)
	}
}

func TestWorker_HandleMessage_InvalidJSON(t *testing.T) {
	t.Parallel()

	w := &Worker{
		sender: &fakeSender{},
		config: newTestConfig(t),
	}

	if err := w.handleMessage(context.Background(), []byte(`{not-json}`)); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestWorker_HandleMessage_EmptyEmail(t *testing.T) {
	t.Parallel()

	w := &Worker{
		sender: &fakeSender{},
		config: newTestConfig(t),
	}

	data := []byte(`{"name":"Ivan","email":"   ","code":"123456"}`)
	if err := w.handleMessage(context.Background(), data); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestWorker_HandleMessage_EmptyCode(t *testing.T) {
	t.Parallel()

	w := &Worker{
		sender: &fakeSender{},
		config: newTestConfig(t),
	}

	data := []byte(`{"name":"Ivan","email":"user@example.com","code":" "}`)
	if err := w.handleMessage(context.Background(), data); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestWorker_HandleMessage_SenderError(t *testing.T) {
	t.Parallel()

	s := &fakeSender{err: errors.New("smtp down")}
	cfg := newTestConfig(t)
	w := &Worker{
		sender: s,
		config: cfg,
	}

	key, err := fernet.DecodeKey(cfg.EmailSecretKey)
	if err != nil {
		t.Fatalf("failed to decode key: %v", err)
	}

	encryptedToken, err := fernet.EncryptAndSign([]byte("123456"), key)
	if err != nil {
		t.Fatalf("failed to encrypt code: %v", err)
	}

	data := []byte(`{"name":"Ivan","email":"user@example.com","code":"` + string(encryptedToken) + `"}`)
	if err := w.handleMessage(context.Background(), data); err == nil {
		t.Fatalf("expected error, got nil")
	}
	if s.calls != 1 {
		t.Fatalf("expected 1 send call, got %d", s.calls)
	}
}

func TestWorker_Run_ClosesConsumerOnExit(t *testing.T) {
	t.Parallel()

	cons := &fakeConsumer{
		msgs: [][]byte{
			[]byte(`{"name":"Ivan","email":"user@example.com","code":"123456"}`),
		},
		closedCh: make(chan struct{}),
	}
	s := &fakeSender{}
	w := &Worker{
		sender:   s,
		consumer: cons,
		config:   newTestConfig(t),
		log:      &nopLogger{},
		workers:  1,
		bufSize:  1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = w.Run(ctx)

	if !cons.closed {
		t.Fatalf("expected consumer to be closed")
	}
}

type nopLogger struct{}

var _ logging.Logger = (*nopLogger)(nil)

func (n *nopLogger) Debug(context.Context, string, ...any) {}
func (n *nopLogger) Info(context.Context, string, ...any)  {}
func (n *nopLogger) Warn(context.Context, string, ...any)  {}
func (n *nopLogger) Error(context.Context, string, ...any) {}
func (n *nopLogger) With(...any) logging.Logger            { return n }
