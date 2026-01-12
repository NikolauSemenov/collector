package utils

import (
	"fmt"
	"strings"

	"github.com/fernet/fernet-go"
)

func EncryptCode(secretKey, code string) (string, error) {
	key, err := fernet.DecodeKey(secretKey)
	if err != nil {
		return "", err
	}

	code = strings.TrimSpace(code)
	if code == "" {
		return "", fmt.Errorf("code is empty")
	}

	token, err := fernet.EncryptAndSign([]byte(code), key)
	if err != nil {
		return "", err
	}

	return string(token), nil
}
