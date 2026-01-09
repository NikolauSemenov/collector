package utils

import (
	"fmt"

	"github.com/fernet/fernet-go"
)

var EmailSecretCode string = "ra1dkbpJGiphNLlHlbfstkTyoNvamAcIf-vq6dOzwoY="

func EncryptCode(code string) (string, error) {

	key, err := fernet.DecodeKey(EmailSecretCode)
	if err != nil {
		return "", err
	}
	msg, _ := fernet.EncryptAndSign(
		[]byte(code),
		key,
	)

	if msg == nil {
		return "", fmt.Errorf("invalid or expired token")
	}

	return string(msg), nil
}
