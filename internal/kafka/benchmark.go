package kafka

import (
	"context"
	"crypto/rand"
	"log"
	"stream/internal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Benchmark sends specified number of messages against Kafka and returns the Benchmark type as result
func Benchmark(
	kafkaConfig kafka.ConfigMap,
	duration int,
	sizeOfMessage int,
) internal.Benchmark {
	producer, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	done := make(chan bool)

	messageCount := 0
	errorCount := 0

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(duration)*time.Second,
	)

	defer cancel()

	go func() {
		for e := range producer.Events() {
			msg := e.(*kafka.Message)

			if msg.TopicPartition.Error != nil {
				errorCount++
				log.Printf("Error: %v", msg.TopicPartition.Error)
				continue
			}
			messageCount++
		}
	}()

	value := make([]byte, sizeOfMessage)
	rand.Read(value)

	topic := "myTopic"

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			done <- true
			goto Complete
		default:
			producer.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic},
				Value:          value,
			}
		}
	}

Complete:
	<-done

	producer.Flush(5000)

	return internal.NewBenchmark(
		time.Since(start),
		messageCount,
		sizeOfMessage,
		errorCount,
	)
}
