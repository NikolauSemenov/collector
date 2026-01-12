package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"collector/internal/config"
)

type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
	With(args ...any) Logger
}

type slogLogger struct {
	l *slog.Logger
}

func New(cfg *config.Config) (Logger, error) {
	level, err := ParseLevel(cfg)
	if err != nil {
		return nil, err
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})

	return &slogLogger{l: slog.New(h)}, nil
}

func ParseLevel(cfg *config.Config) (slog.Level, error) {
	if cfg == nil {
		return slog.LevelInfo, fmt.Errorf("config is nil")
	}

	v := strings.TrimSpace(strings.ToLower(cfg.LogLevel))
	switch v {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid LOG_LEVEL: %q", cfg.LogLevel)
	}
}

func (s *slogLogger) Debug(ctx context.Context, msg string, args ...any) {
	s.l.DebugContext(ctx, msg, args...)
}
func (s *slogLogger) Info(ctx context.Context, msg string, args ...any) {
	s.l.InfoContext(ctx, msg, args...)
}
func (s *slogLogger) Warn(ctx context.Context, msg string, args ...any) {
	s.l.WarnContext(ctx, msg, args...)
}
func (s *slogLogger) Error(ctx context.Context, msg string, args ...any) {
	s.l.ErrorContext(ctx, msg, args...)
}

func (s *slogLogger) With(args ...any) Logger {
	return &slogLogger{l: s.l.With(args...)}
}
