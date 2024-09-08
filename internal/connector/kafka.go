package connector

import (
	"encoding/json"
	"log"
	"math/rand"
	"stream/internal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type producerMessage struct {
	Number    float64   `json:"number"`
	CreatedAt time.Time `json:"createdAt"`
}

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

func ProduceKafka(kafkaConfig kafka.ConfigMap, frequency int, max int) {
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
			&producerMessage{Number: randNumber, CreatedAt: time.Now()},
		)

		log.Printf("Producing message: %s\n", string(message))
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          []byte(string(message)),
		}, nil)

		time.Sleep(time.Duration(frequency) * time.Millisecond)
	}
}

func ConsumeKafka(kafkaConfig kafka.ConfigMap, frequency int) {
	consumer, err := kafka.NewConsumer(&kafkaConfig)

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	consumer.Subscribe("produce", nil)

	run := true
	for run {
		ev := consumer.Poll(frequency) // Poll frequency is in [ms]
		switch e := ev.(type) {
		case *kafka.Message:
			message := producerMessage{}

			json.Unmarshal([]byte(string(e.Value)), &message)

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
