package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)


func ConnectKafka(kafkaConfig kafka.ConfigMap, numberOfMessages int, sizeOfMessage int) Benchmark {
    p, err := kafka.NewProducer(&kafkaConfig)
    if err != nil {
        log.Fatal(err)
    }

    done := make(chan bool)

    go func() {
        var sentCount int
        for e := range p.Events() {
            msg := e.(*kafka.Message)

            if msg.TopicPartition.Error != nil {
                log.Printf("delivery report error: %v", msg.TopicPartition.Error)
                os.Exit(1)
            }
            sentCount++
            if sentCount >= numberOfMessages {
                done <- true
            }
        }
    }()

    defer p.Close()

    value := make([]byte, sizeOfMessage)
    rand.Read(value)

    topic := "myTopic"

    start := time.Now()
    for i := 0; i < numberOfMessages; i++ {
        p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic}, Value: value}
    }
    <-done

    duration := time.Since(start)

    p.Flush(5000)

    return NewBenchmark(duration, numberOfMessages, sizeOfMessage)
}

type producerMessage struct {
    Number float64 `json:"number"`
}

func ProduceKafka(kafkaConfig kafka.ConfigMap, max int) {
    producer, err := kafka.NewProducer(&kafkaConfig)

    topic := "produce"

    if err != nil {
        log.Fatal(err)
    }

    defer producer.Close()

    for {
        randInt := rand.Intn(max)
        randFloat := rand.Float64()

        randNumber := float64(randInt) + randFloat

        message, _ := json.Marshal(&producerMessage{Number: randNumber})

        fmt.Println(string(message))
        producer.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic},
            Value: []byte(string(message)),
        }, nil)

        time.Sleep(1 * time.Second)
    }
}

func ConsumeKafka(kafkaConfig kafka.ConfigMap) {
    consumer, err := kafka.NewConsumer(&kafkaConfig)

    if err != nil {
        log.Fatal(err)
    }

    defer consumer.Close()

    consumer.Subscribe("produce", nil)

    for {
        message, err := consumer.ReadMessage(time.Second)

        if err == nil {
            log.Printf("Message on %s: %s\n", message.TopicPartition, string(message.Value))
        } else {
            fmt.Printf("Consumer error: %v (%v)\n", err, message)
        }
    }
}
