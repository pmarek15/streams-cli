package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"stream/internal"

	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func ConnectPulsar(
	pulsarConfig internal.PulsarConfig,
	numberOfMessages int,
	sizeOfMessage int,
) internal.Benchmark {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarConfig.Url,
	})

	if err != nil {
		log.Fatal(err)
	}

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

	for i := 0; i < numberOfMessages; i++ {
		producer.SendAsync(
			ctx,
			&msg,
			func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				if err != nil {
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

	return internal.NewBenchmark(duration, numberOfMessages, sizeOfMessage)
}

func ProducePulsar(pulsarConfig internal.PulsarConfig, frequency int, max int) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarConfig.Url,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "produce",
	})

	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	ctx := context.Background()

	for {
		randInt := rand.Intn(max)
		randFloat := rand.Float64()

		randNumber := float64(randInt) + randFloat

		jsonMessage, _ := json.Marshal(
			&producerMessage{Number: randNumber, CreatedAt: time.Now()},
		)

		msg := pulsar.ProducerMessage{
			Payload: []byte(string(jsonMessage)),
		}

		producer.SendAsync(
			ctx,
			&msg,
			func(_ pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
				if err != nil {
					log.Printf("Produce message err: %v\n", err)
				}
				fmt.Println(string(msg.Payload))
			},
		)

		time.Sleep(time.Duration(frequency) * time.Millisecond)
	}
}

func ConsumePulsar(pulsarConfig internal.PulsarConfig) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarConfig.Url,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	channel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "produce",
		SubscriptionName: "mySubscription",
		MessageChannel:   channel,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	for consumedMessage := range channel {
		payload := consumedMessage.Message.Payload()

		message := producerMessage{}
		json.Unmarshal([]byte(string(payload)), &message)

		fmt.Printf(
			"Received message with ID: %v -- content: '%s'\n",
			consumedMessage.Message.ID(),
			string(consumedMessage.Message.Payload()),
		)

		fmt.Printf(
			"Time between producing and consuming [ms]: %d\n\n",
			time.Since(message.CreatedAt).Milliseconds(),
		)

		consumer.Ack(consumedMessage.Message)
	}
}
