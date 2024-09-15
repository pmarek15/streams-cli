package kafka

import (
	"encoding/json"
	"log"
	"math/rand"
	"stream/internal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Produce(kafkaConfig kafka.ConfigMap, frequency int, max int) {
	producer, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	topic := "produce"
	for {
		randInt := rand.Intn(max)
		randFloat := rand.Float64()

		randNumber := float64(randInt) + randFloat

		message, _ := json.Marshal(
			&internal.Message{Number: randNumber, CreatedAt: time.Now()},
		)

		log.Printf("Producing message: %s\n", string(message))

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          message,
		}, nil)

		if err != nil {
			log.Fatalf("Error producing message: %s\n", err)
		}

		time.Sleep(time.Duration(frequency) * time.Millisecond)
	}
}