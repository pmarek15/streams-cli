package kafka

import (
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Consume(kafkaConfig kafka.ConfigMap, frequency int) {
	consumer, err := kafka.NewConsumer(&kafkaConfig)

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	err = consumer.Subscribe("produce", nil)
	if err != nil {
		log.Fatalf(
			"Failed to subscribe to the producer: %s",
			err,
		)
		return
	}

	run := true
	for run {
		ev := consumer.Poll(frequency) // Poll frequency is in [ms]
		switch e := ev.(type) {
		case *kafka.Message:
			message := message{}

			_ = json.Unmarshal(e.Value, &message)

			log.Printf(
				"Message on %s: %s\nLatency [ms]: %d\n\n",
				e.TopicPartition,
				string(e.Value),
				time.Since(message.CreatedAt).Milliseconds(),
			)
		case kafka.Error:
			log.Printf("Error: %v\n", e)
			run = false
		case kafka.PartitionEOF:
			log.Fatalf("Reached %v\n", e)
		}
	}
}
