package internal

import (
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)


func ConnectKafka(BootstrapServers string, numberOfMessages int, sizeOfMessage int) {
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": BootstrapServers})

    if err != nil {
        panic(err)
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

    elapsed := time.Since(start)

    messagesPerSecond := (float64(numberOfMessages) / elapsed.Seconds())
    throughput := (float64(numberOfMessages) * float64(sizeOfMessage)) / elapsed.Seconds()
    fmt.Printf("Time taken [s]: %f\nMessages/s: %f\nThroughput [MB/s]: %.2f\n", elapsed.Seconds(), messagesPerSecond, throughput / 1000000)
}
