package main

import (
	"collector/internal/app"
	"collector/internal/config"
	"collector/internal/logging"
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
)

const (
	healthCheckPort = ":8000"
	exitCodeError   = 1
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		os.Exit(exitCodeError)
	}

	l, err := logging.New(cfg)
	if err != nil {
		os.Exit(exitCodeError)
	}
	l = l.With("component", "main")

	health, err := StartTCPHealthListener(ctx, healthCheckPort)
	if err != nil {
		l.Error(ctx, "failed to start tcp health listener", "addr", healthCheckPort, "err", err)
		os.Exit(exitCodeError)
	}
	defer func() {
		if err := health.Close(); err != nil {
			l.Error(ctx, "failed to close health listener", "err", err)
		}
	}()

	go func() {
		for err := range health.Errors() {
			if err != nil {
				l.Error(ctx, "tcp health listener error", "err", err)
			}
		}
	}()

	l.Info(ctx, "health check listener started", "addr", healthCheckPort)

	w, err := app.NewWorker()
	if err != nil {
		l.Error(ctx, "failed to create worker", "err", err)
		os.Exit(exitCodeError)
	}

	l.Info(ctx, "starting worker")

	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		l.Error(ctx, "worker stopped with error", "err", err)
		os.Exit(exitCodeError)
	}

	l.Info(ctx, "worker stopped gracefully")
}
