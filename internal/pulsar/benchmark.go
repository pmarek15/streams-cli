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
	numberOfMessages int,
	sizeOfMessage int,
) internal.Benchmark {
	client := GetClient(pulsarConfig)

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:              "myTopic",
		MaxPendingMessages: numberOfMessages * 2,
	})

	if err != nil {
		log.Fatal(err)
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

	errorCount := 0

	for i := 0; i < numberOfMessages; i++ {
		producer.SendAsync(
			ctx,
			&msg,
			func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				if err != nil {
					errorCount++
					log.Fatal(err)
				}

				messagesSent++

				if messagesSent >= numberOfMessages {
					done <- true
				}
			},
		)
	}

	<-done
	duration := time.Since(start)

	producer.Flush()

	return internal.NewBenchmark(duration, numberOfMessages, sizeOfMessage, errorCount)
}
