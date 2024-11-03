package kafka

import (
	"context"
	"crypto/rand"
	"log"
	"stream/internal"
	"sync/atomic"
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

	messageCount := uint64(0)
	errorCount := uint64(0)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(duration)*time.Second,
	)

	defer cancel()

	go func() {
		for e := range producer.Events() {
			msg := e.(*kafka.Message)

			if msg.TopicPartition.Error != nil {
				atomic.AddUint64(&errorCount, 1)
				log.Printf("Error: %v", msg.TopicPartition.Error)
				continue
			}
			atomic.AddUint64(&messageCount, 1)
		}
	}()

	value := make([]byte, sizeOfMessage)
	rand.Read(value)

	topic := "myTopic"

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			goto Complete
		default:
			producer.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic},
				Value:          value,
			}
		}
	}

Complete:
	producer.Flush(5000)

	return internal.NewBenchmark(
		time.Since(start),
		int(atomic.LoadUint64(&messageCount)),
		sizeOfMessage,
		int(atomic.LoadUint64(&errorCount)),
	)
}
