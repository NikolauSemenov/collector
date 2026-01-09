package email

import (
	"bytes"
	"collector/internal/config"
	"html/template"

	"gopkg.in/gomail.v2"
)

type SenderEmail struct {
	config *config.Config
}

func NewSenderEmail(config *config.Config) (*SenderEmail, error) {
	return &SenderEmail{config: config}, nil
}

func (s *SenderEmail) Send(email, header, text string) error {
	tpl, err := template.New("email").Parse(HtmlBodyTpl)
	if err != nil {
		return err
	}

	var htmlBuf bytes.Buffer
	if err = tpl.Execute(&htmlBuf, struct {
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
	m.SetHeader("Subject", "Письмо от сервиса Auth")

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
