package app

import (
	"collector/internal/config"
	"collector/internal/email"
	"collector/internal/infrastructure/broker"
	"collector/internal/logging"
	"collector/internal/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	defaultBufferSize = 256

	defaultReadBackoff = 50 * time.Millisecond

	defaultSendMaxRetries = 3
	defaultSendRetryBase  = 200 * time.Millisecond

	emailSubject    = "Письмо от сервиса Auth"
	emailBodyPrefix = "Код доступа: "
)

type EmailEventSchema struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Code  string `json:"code"`
}

type Worker struct {
	sender   email.Sender
	consumer broker.MessageConsumer
	config   *config.Config
	log      logging.Logger

	workers int
	bufSize int

	readBackoff    time.Duration
	sendMaxRetries int
	sendRetryBase  time.Duration
}

func NewWorker() (*Worker, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	l, err := logging.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}

	sender, err := email.NewSenderEmail(cfg)
	if err != nil {
		return nil, fmt.Errorf("create email sender: %w", err)
	}

	consumer, err := broker.NewConsumerKafka(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	return &Worker{
		sender:   sender,
		consumer: consumer,
		config:   cfg,
		log:      l.With("component", "worker"),
		workers:  max(runtime.GOMAXPROCS(0), 1),
		bufSize:  defaultBufferSize,

		readBackoff:    defaultReadBackoff,
		sendMaxRetries: defaultSendMaxRetries,
		sendRetryBase:  defaultSendRetryBase,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if w == nil || w.consumer == nil || w.sender == nil || w.config == nil || w.log == nil {
		return fmt.Errorf("worker is not initialized")
	}

	if w.readBackoff <= 0 {
		w.readBackoff = defaultReadBackoff
	}
	if w.sendMaxRetries <= 0 {
		w.sendMaxRetries = defaultSendMaxRetries
	}
	if w.sendRetryBase <= 0 {
		w.sendRetryBase = defaultSendRetryBase
	}

	w.log.Info(ctx, "worker starting", "num_workers", w.workers, "buffer_size", w.bufSize)

	defer func() {
		if err := w.consumer.Close(); err != nil {
			w.log.Error(ctx, "failed to close consumer", "err", err)
		}
	}()

	msgCh := make(chan []byte, w.bufSize)

	readErrCh := make(chan error, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.readLoop(ctx, msgCh, readErrCh)
	}()

	for i := range w.workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w.workerLoop(ctx, workerID, msgCh)
		}(i)
	}

	select {
	case <-ctx.Done():
		w.log.Info(ctx, "context cancelled, shutting down")
	case err := <-readErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			w.log.Error(ctx, "read loop stopped with error", "err", err)
		}
	}

	wg.Wait()
	w.log.Info(ctx, "all workers stopped")

	select {
	case err := <-readErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	default:
	}

	return ctx.Err()
}

func (w *Worker) readLoop(ctx context.Context, out chan []byte, errCh chan<- error) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			w.log.Info(ctx, "read loop stopping")
			return
		default:
		}

		msg, err := w.consumer.ReadMessage()
		if err != nil {
			w.log.Error(ctx, "failed to read message from consumer", "err", err)
			select {
			case errCh <- err:
			default:
			}
			return
		}

		if len(msg) == 0 {
			select {
			case <-ctx.Done():
				w.log.Info(ctx, "read loop stopping")
				return
			case <-time.After(w.readBackoff):
				continue
			}
		}

		select {
		case out <- msg:
		case <-ctx.Done():
			w.log.Info(ctx, "read loop stopping during message send")
			return
		}
	}
}

func (w *Worker) workerLoop(ctx context.Context, id int, in <-chan []byte) {
	l := w.log.With("worker_id", id)
	l.Info(ctx, "worker started")
	defer l.Info(ctx, "worker stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-in:
			if !ok {
				return
			}

			if err := w.handleMessage(ctx, data); err != nil {
				l.Error(ctx, "handle message failed", "err", err)
			}

			if err := w.consumer.Commit(); err != nil {
				l.Error(ctx, "commit failed", "err", err)
			}
		}
	}
}

func (w *Worker) handleMessage(ctx context.Context, data []byte) error {
	var event EmailEventSchema
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	event.Email = strings.TrimSpace(event.Email)
	if event.Email == "" {
		return fmt.Errorf("event email is empty")
	}

	encryptedCode := strings.TrimSpace(event.Code)
	if encryptedCode == "" {
		return fmt.Errorf("event code is empty")
	}

	decryptedCode, err := utils.DecryptCode(w.config.EmailSecretKey, encryptedCode)
	if err != nil {
		return fmt.Errorf("decrypt code: %w", err)
	}

	body := emailBodyPrefix + decryptedCode

	var lastErr error
	for attempt := 0; attempt <= w.sendMaxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt > 0 {
			backoff := w.sendRetryBase * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		if err := w.sender.Send(event.Email, emailSubject, body); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("send email: %w", lastErr)
}
