package utils

import (
	"fmt"
	"strings"

	"github.com/fernet/fernet-go"
)

func DecryptCode(secretKey, token string) (string, error) {
	key, err := fernet.DecodeKey(secretKey)
	if err != nil {
		return "", err
	}

	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("token is empty")
	}

	decoded := fernet.VerifyAndDecrypt([]byte(token), 0, []*fernet.Key{key})
	if decoded == nil {
		return "", fmt.Errorf("failed to decrypt token")
	}

	return string(decoded), nil
}
