package config

import (
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	BrokerConfig
	EmailConfig
}

type BrokerConfig struct {
	Servers       string        `env:"BROKER_BROKERS" env-default:"localhost:29092"`
	TopicEmail    string        `env:"BROKER_TOPIC_EMAIL" env-default:"verify_email_events"`
	ConsumerGroup string        `env:"BROKER_CONSUMER_GROUP" env-default:"email.smtp"`
	Timeout       time.Duration `env:"BROKER_TIMEOUT" env-default:"5s"`
}

type EmailConfig struct {
	EmailHost     string `env:"EMAIL_HOST" env-default:"smtp.gmail.com"`
	EmailPort     int    `env:"EMAIL_PORT" env-default:"465"`
	EmailUsername string `env:"EMAIL_USERNAME" env-default:"nikolausemenov1@gmail.com"`
	EmailPassword string `env:"EMAIL_PASSWORD" env-default:"vjpy mgrx pcrr xjiw"`
}

func GetConfig() *Config {
	var cfg Config
	errEnv := cleanenv.ReadEnv(&cfg)
	if errEnv != nil {
		return nil
	}
	return &cfg
}
