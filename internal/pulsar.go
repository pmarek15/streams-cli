package internal

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func ConnectPulsar(url string, numberOfMessages int, sizeOfMessage int) {
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: url,
    })

    if err != nil {
        panic(err)
    }

    defer client.Close()


    producer, err := client.CreateProducer(pulsar.ProducerOptions{
        Topic: "myTopic",
    })

    if err != nil {
        panic(err)
    }

    defer producer.Close()

    value := make([]byte, sizeOfMessage)
    rand.Read(value)


    messagesSent := 0

    ctx := context.Background()
    start := time.Now()
    
    for i := 0; i < numberOfMessages; i++ {
        msg := pulsar.ProducerMessage{
            Payload: value,
        }

        producer.SendAsync(ctx, &msg, func(mi pulsar.MessageID, pm *pulsar.ProducerMessage, err error) {
            if err != nil {
                log.Fatal(err)
            }

            messagesSent++
        })
    }

    elapsed := time.Since(start)

    messagesPerSecond := (float64(numberOfMessages) / elapsed.Seconds())
    throughput := (float64(numberOfMessages) * float64(sizeOfMessage)) / elapsed.Seconds()
    fmt.Printf("Time taken [s]: %f\nMessages/s: %f\nThroughput [MB/s]: %.2f\n", elapsed.Seconds(), messagesPerSecond, throughput / 1000000)
}
