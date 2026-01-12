package utils

import (
	"strings"
	"testing"

	"github.com/fernet/fernet-go"
)

// TestEncryptCode_ProducesToken verifies that EncryptCode returns a non-empty Fernet token.
func TestEncryptCode_ProducesToken(t *testing.T) {
	t.Parallel()

	secretKey := newFernetKey(t).Encode()

	token, err := EncryptCode(secretKey, "123456")
	if err != nil {
		t.Fatalf("EncryptCode returned error: %v", err)
	}
	if strings.TrimSpace(token) == "" {
		t.Fatalf("expected non-empty token")
	}
}

// TestEncryptCode_EmptyCode ensures EncryptCode rejects an empty/whitespace-only code value.
func TestEncryptCode_EmptyCode(t *testing.T) {
	t.Parallel()

	secretKey := newFernetKey(t).Encode()

	if _, err := EncryptCode(secretKey, ""); err == nil {
		t.Fatalf("expected error, got nil")
	}
	if _, err := EncryptCode(secretKey, "   "); err == nil {
		t.Fatalf("expected error for whitespace-only code, got nil")
	}
}

// TestEncryptCode_InvalidKey ensures EncryptCode fails on an invalid secret key.
func TestEncryptCode_InvalidKey(t *testing.T) {
	t.Parallel()

	if _, err := EncryptCode("not-a-fernet-key", "123456"); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// newFernetKey generates a fresh Fernet key for testing.
func newFernetKey(t *testing.T) *fernet.Key {
	t.Helper()

	var k fernet.Key
	k.Generate()
	return &k
}
