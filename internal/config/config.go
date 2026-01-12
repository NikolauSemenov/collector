package config

import (
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

type Config struct {
	BrokerConfig
	EmailConfig
	LogConfig
}

type LogConfig struct {
	LogLevel string `env:"LOG_LEVEL"`
}

type BrokerConfig struct {
	Servers       string        `env:"BROKER_BROKERS"`
	TopicEmail    string        `env:"BROKER_TOPIC_EMAIL"`
	ConsumerGroup string        `env:"BROKER_CONSUMER_GROUP"`
	Timeout       time.Duration `env:"BROKER_TIMEOUT"`
}

type EmailConfig struct {
	EmailHost      string `env:"EMAIL_HOST"`
	EmailPort      int    `env:"EMAIL_PORT"`
	EmailUsername  string `env:"EMAIL_USERNAME"`
	EmailPassword  string `env:"EMAIL_PASSWORD"`
	EmailSecretKey string `env:"EMAIL_SECRET_KEY"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	var cfg Config
	errEnv := cleanenv.ReadEnv(&cfg)
	if errEnv != nil {
		return nil, errEnv
	}
	return &cfg, nil
}
