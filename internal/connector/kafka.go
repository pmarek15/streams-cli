package connector

import (
	"log"
	"math/rand"
	"stream/internal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func ConnectKafka(
	kafkaConfig kafka.ConfigMap,
	numberOfMessages int,
	sizeOfMessage int,
) internal.Benchmark {
	p, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	done := make(chan bool)

	go func() {
		var sentCount int
		for e := range p.Events() {
			msg := e.(*kafka.Message)

			if msg.TopicPartition.Error != nil {
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
		p.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          value,
		}
	}
	<-done

	duration := time.Since(start)

	p.Flush(5000)

	return internal.NewBenchmark(duration, numberOfMessages, sizeOfMessage)
}
