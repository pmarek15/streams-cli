package pulsar

import (
	"context"
	"log"
	"math/rand"
	"stream/internal"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func Benchmark(
	pulsarConfig internal.PulsarConfig,
	duration int,
	sizeOfMessage int,
) internal.Benchmark {
	client := GetClient(pulsarConfig)

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:              "myTopic",
		MaxPendingMessages: 10000,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	value := make([]byte, sizeOfMessage)
	rand.Read(value)

	messagesSent := 0
	errorCount := 0
	done := make(chan bool)
	completionChan := make(chan bool)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Second)

	defer cancel()

	msg := pulsar.ProducerMessage{
		Payload: value,
	}

	start := time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- true
				return
			default:
				producer.SendAsync(
					ctx,
					&msg,
					func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
						if err != nil {
							errorCount++
							log.Printf("Error sending message: %v", err)
							completionChan <- true
							return
						}
						messagesSent++
						completionChan <- true
					},
				)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-completionChan:
				continue
			}
		}
	}()

	<-done

	producer.Flush()

	return internal.NewBenchmark(time.Since(start), messagesSent, sizeOfMessage, errorCount)
}
