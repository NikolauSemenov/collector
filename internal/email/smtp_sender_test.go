package email

import (
	"collector/internal/config"
	"strings"
	"testing"
)

func TestNewSenderEmail_NilConfig(t *testing.T) {
	t.Parallel()

	s, err := NewSenderEmail(nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if s != nil {
		t.Fatalf("expected nil sender, got non-nil")
	}
}

func TestSend_EmptyEmail(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		EmailConfig: config.EmailConfig{
			EmailHost:      "smtp.example.com",
			EmailPort:      465,
			EmailUsername:  "user@example.com",
			EmailPassword:  "app-password",
			EmailSecretKey: "not-used-here",
		},
	}

	s, err := NewSenderEmail(cfg)
	if err != nil {
		t.Fatalf("expected nil error from NewSenderEmail, got %v", err)
	}

	if err := s.Send("", "Subject", "Body"); err == nil {
		t.Fatalf("expected error for empty email, got nil")
	}
	if err := s.Send("   ", "Subject", "Body"); err == nil {
		t.Fatalf("expected error for whitespace email, got nil")
	}
}

func TestSend_UsesProvidedSubject(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		EmailConfig: config.EmailConfig{
			EmailHost:      "smtp.invalid",
			EmailPort:      465,
			EmailUsername:  "user@example.com",
			EmailPassword:  "app-password",
			EmailSecretKey: "not-used-here",
		},
	}

	s, err := NewSenderEmail(cfg)
	if err != nil {
		t.Fatalf("expected nil error from NewSenderEmail, got %v", err)
	}

	subject := "My Custom Subject"
	err = s.Send("recipient@example.com", subject, "Hello")
	if err == nil {
		t.Fatalf("expected error due to unreachable SMTP host, got nil")
	}

	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "subject") && strings.Contains(lower, "empty") {
		t.Fatalf("unexpected error that suggests subject was empty: %v", err)
	}
}

func TestNewSenderEmail_ParsesTemplate(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		EmailConfig: config.EmailConfig{
			EmailHost:      "smtp.example.com",
			EmailPort:      465,
			EmailUsername:  "user@example.com",
			EmailPassword:  "app-password",
			EmailSecretKey: "not-used-here",
		},
	}

	s, err := NewSenderEmail(cfg)
	if err != nil {
		t.Fatalf("expected nil error from NewSenderEmail, got %v", err)
	}
	if s == nil {
		t.Fatalf("expected non-nil sender")
	}
	if s.tpl == nil {
		t.Fatalf("expected parsed template to be non-nil")
	}
}
