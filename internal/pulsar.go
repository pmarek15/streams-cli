package internal

import (
	"context"
	"crypto/rand"

	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func ConnectPulsar(pulsarConfig PulsarConfig, numberOfMessages int, sizeOfMessage int) Benchmark {
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: pulsarConfig.Url,
    })

    if err != nil {
        panic(err)
    }

    defer client.Close()


    producer, err := client.CreateProducer(pulsar.ProducerOptions{
        Topic: "myTopic",
        MaxPendingMessages: numberOfMessages*2,
    })

    if err != nil {
        panic(err)
    }

    defer producer.Close()

    value := make([]byte, sizeOfMessage)
    rand.Read(value)

    messagesSent := 0
    ctx := context.Background()
    done := make(chan bool)

    msg := pulsar.ProducerMessage{
        Payload: value,
    }

    start := time.Now()    

     for i := 0; i < numberOfMessages; i++ {
        producer.SendAsync(ctx, &msg, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
            if err != nil {
                log.Fatal(err)
            }

            messagesSent++

            if messagesSent >= numberOfMessages {
                done <- true
            }
        })
    }

    <-done
    duration := time.Since(start)

    producer.Flush()

    return NewBenchmark(duration, numberOfMessages, sizeOfMessage)
}
