package internal

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v3"
)

type PulsarConfig struct {
	Url string `yaml:"url"`
}

type Config struct {
	KafkaConfig    kafka.ConfigMap `yaml:"kafka"`
	PulsarConfig   PulsarConfig    `yaml:"pulsar"`
	RedpandaConfig kafka.ConfigMap `yaml:"redpanda"`
}

func ParseConfig(file string) (Config, error) {
	rawData, err := os.ReadFile(file)

	if err != nil {
		return Config{}, err
	}

	var configData Config

	err = yaml.Unmarshal(rawData, &configData)

	if err != nil {
		return Config{}, err
	}

	return configData, nil
}
