package config

import (
	"testing"
)

func TestLoad_MissingRequiredEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("BROKER_BROKERS", "localhost:29092")
	t.Setenv("BROKER_TOPIC_EMAIL", "verify_email_events")
	t.Setenv("BROKER_CONSUMER_GROUP", "email.smtp")
	t.Setenv("BROKER_TIMEOUT", "5s")
	t.Setenv("EMAIL_HOST", "smtp.gmail.com")
	t.Setenv("EMAIL_PORT", "465")
	t.Setenv("EMAIL_USERNAME", "")
	t.Setenv("EMAIL_PASSWORD", "")
	t.Setenv("EMAIL_SECRET_KEY", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() should not fail: %v", err)
	}
	if cfg.EmailUsername != "" || cfg.EmailPassword != "" || cfg.EmailSecretKey != "" {
		t.Fatalf("expected empty credentials")
	}
}

func TestLoad_UsesDefaultsForNonRequired(t *testing.T) {
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("BROKER_BROKERS", "localhost:29092")
	t.Setenv("BROKER_TOPIC_EMAIL", "verify_email_events")
	t.Setenv("BROKER_CONSUMER_GROUP", "email.smtp")
	t.Setenv("BROKER_TIMEOUT", "5s")
	t.Setenv("EMAIL_HOST", "smtp.gmail.com")
	t.Setenv("EMAIL_PORT", "465")
	t.Setenv("EMAIL_USERNAME", "user@example.com")
	t.Setenv("EMAIL_PASSWORD", "app-password")
	t.Setenv("EMAIL_SECRET_KEY", "some-secret-key")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if cfg == nil {
		t.Fatalf("expected non-nil config")
	}

	if cfg.EmailUsername != "user@example.com" {
		t.Fatalf("expected EMAIL_USERNAME to be loaded, got %q", cfg.EmailUsername)
	}
	if cfg.EmailPassword != "app-password" {
		t.Fatalf("expected EMAIL_PASSWORD to be loaded, got %q", cfg.EmailPassword)
	}
	if cfg.EmailSecretKey != "some-secret-key" {
		t.Fatalf("expected EMAIL_SECRET_KEY to be loaded, got %q", cfg.EmailSecretKey)
	}

	if cfg.Servers == "" || cfg.TopicEmail == "" || cfg.ConsumerGroup == "" {
		t.Fatalf("expected broker defaults to be populated, got servers=%q topic=%q group=%q",
			cfg.Servers, cfg.TopicEmail, cfg.ConsumerGroup)
	}
	if cfg.Timeout <= 0 {
		t.Fatalf("expected positive default timeout, got %v", cfg.Timeout)
	}
}
