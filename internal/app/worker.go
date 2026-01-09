package app

import (
	"collector/internal/config"
	"collector/internal/email"
	"collector/internal/infrastructure/broker"
	"context"
	"encoding/json"
	"fmt"
	"sync"
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
	workers  int
}

func NewWorker() (*Worker, error) {
	cfg := config.GetConfig()

	sender, err := email.NewSenderEmail(cfg)
	if err != nil {
		return nil, err
	}

	consumer, errCons := broker.NewConsumerKafka(cfg)
	if errCons != nil {
		return nil, errCons
	}

	return &Worker{
		sender:   sender,
		consumer: consumer,
		config:   cfg,
		workers:  5,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	defer w.consumer.Close()

	msgCh := make(chan []byte, 100)

	var wg sync.WaitGroup

	wg.Add(1)
	// В отдельном потоке читаем данные из брокера
	go func() {
		defer wg.Done()
		w.readLoop(ctx, msgCh)
	}()

	// В нескольких потоках обрабатываем сообщения от брокера
	for i := 0; i < w.workers; i++ {
		wg.Add(1)
		go func(id int) {
			fmt.Println("Start worker", id)
			defer wg.Done()
			w.workerLoop(ctx, msgCh)
		}(i)
	}

	wg.Wait()
	return nil
}

func (w *Worker) readLoop(ctx context.Context, out chan<- []byte) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// TODO: Переделать на Pool, чтобы получать пачками сообщения
		msg, err := w.consumer.ReadMessage()
		if err != nil {
			fmt.Println("kafka read error:", err)
			continue
		}
		if msg == nil {
			continue
		}

		select {
		case out <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) workerLoop(ctx context.Context, in <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-in:
			if !ok {
				return
			}
			w.handleMessage(data)
		}
	}
}

func (w *Worker) handleMessage(data []byte) {
	var event EmailEventSchema

	if err := json.Unmarshal(data, &event); err != nil {
		fmt.Println("json error:", err)
		return
	}

	if err := w.sender.Send(
		event.Email,
		"Письмо от сервиса Auth",
		"Код доступа: "+event.Code,
	); err != nil {
		fmt.Println("email send error:", err)
	}
}
