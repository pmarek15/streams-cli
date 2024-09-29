package kafka

import (
	"log"
	"math/rand"
	"stream/internal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Benchmark sends specified number of messages against Kafka and returns the Benchmark type as result
func Benchmark(
	kafkaConfig kafka.ConfigMap,
	numberOfMessages int,
	sizeOfMessage int,
) internal.Benchmark {
	producer, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	done := make(chan bool)

	errorCount := 0

	go func() {
		var sentCount int
		for e := range producer.Events() {
			msg := e.(*kafka.Message)

			if msg.TopicPartition.Error != nil {
				errorCount++
				log.Fatalf("Error: %v", msg.TopicPartition.Error)
			}
			sentCount++
			if sentCount >= numberOfMessages {
				done <- true
			}
		}
	}()

	value := make([]byte, sizeOfMessage)
	rand.Read(value)

	topic := "myTopic"

	start := time.Now()
	for i := 0; i < numberOfMessages; i++ {
		producer.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          value,
		}
	}
	<-done

	duration := time.Since(start)

	producer.Flush(5000)

	return internal.NewBenchmark(duration, numberOfMessages, sizeOfMessage, errorCount)
}
