package email

import (
	"bytes"
	"collector/internal/config"
	"fmt"
	"html/template"
	"strings"

	"gopkg.in/gomail.v2"
)

type SenderEmail struct {
	config *config.Config
	tpl    *template.Template
}

func NewSenderEmail(config *config.Config) (*SenderEmail, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	tpl, err := template.New("email").Parse(HtmlBodyTpl)
	if err != nil {
		return nil, err
	}

	return &SenderEmail{
		config: config,
		tpl:    tpl,
	}, nil
}

func (s *SenderEmail) Send(email, header, text string) error {
	if s == nil || s.config == nil {
		return fmt.Errorf("sender is not initialized")
	}
	if strings.TrimSpace(email) == "" {
		return fmt.Errorf("email is empty")
	}

	var htmlBuf bytes.Buffer
	if err := s.tpl.Execute(&htmlBuf, struct {
		Text   string
		Header string
	}{
		Text:   text,
		Header: header,
	}); err != nil {
		return err
	}

	m := gomail.NewMessage()
	m.SetHeader("From", s.config.EmailUsername)
	m.SetHeader("To", email)
	m.SetHeader("Subject", header)

	m.SetBody("text/plain", text)
	m.AddAlternative("text/html", htmlBuf.String())

	d := gomail.NewDialer(
		s.config.EmailHost,
		s.config.EmailPort,
		s.config.EmailUsername,
		s.config.EmailPassword,
	)

	return d.DialAndSend(m)
}
