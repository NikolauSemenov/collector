package utils

import (
	"testing"

	"github.com/fernet/fernet-go"
)

func TestDecryptCode_ValidToken(t *testing.T) {
	t.Parallel()

	key := newFernetKey(t)
	secretKey := key.Encode()
	originalCode := "123456"

	token, err := fernet.EncryptAndSign([]byte(originalCode), key)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	decrypted, err := DecryptCode(secretKey, string(token))
	if err != nil {
		t.Fatalf("DecryptCode returned error: %v", err)
	}

	if decrypted != originalCode {
		t.Fatalf("expected decrypted code %q, got %q", originalCode, decrypted)
	}
}

func TestDecryptCode_EmptyToken(t *testing.T) {
	t.Parallel()

	secretKey := newFernetKey(t).Encode()

	if _, err := DecryptCode(secretKey, ""); err == nil {
		t.Fatalf("expected error, got nil")
	}
	if _, err := DecryptCode(secretKey, "   "); err == nil {
		t.Fatalf("expected error for whitespace-only token, got nil")
	}
}

func TestDecryptCode_InvalidKey(t *testing.T) {
	t.Parallel()

	if _, err := DecryptCode("not-a-fernet-key", "gAAAAABjtest"); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestDecryptCode_InvalidToken(t *testing.T) {
	t.Parallel()

	secretKey := newFernetKey(t).Encode()

	if _, err := DecryptCode(secretKey, "invalid-token"); err == nil {
		t.Fatalf("expected error for invalid token, got nil")
	}
}

func TestDecryptCode_WrongKey(t *testing.T) {
	t.Parallel()

	key1 := newFernetKey(t)
	key2 := newFernetKey(t)

	token, err := fernet.EncryptAndSign([]byte("123456"), key1)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	if _, err := DecryptCode(key2.Encode(), string(token)); err == nil {
		t.Fatalf("expected error when using wrong key, got nil")
	}
}

func newFernetKey(t *testing.T) *fernet.Key {
	t.Helper()

	var k fernet.Key
	k.Generate()
	return &k
}
