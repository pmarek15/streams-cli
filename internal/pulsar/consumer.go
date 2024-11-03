package pulsar

import (
	"encoding/json"
	"fmt"
	"log"
	"stream/internal"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func Consume(pulsarConfig internal.PulsarConfig) {
	client := GetClient(pulsarConfig)

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

		message := message{}
		json.Unmarshal(payload, &message)

		fmt.Printf(
			"Received message with ID: %v -- content: '%s'\n",
			consumedMessage.Message.ID(),
			string(consumedMessage.Message.Payload()),
		)

		fmt.Printf(
			"Time between producing and consuming [ms]: %d\n\n",
			time.Since(message.CreatedAt).Milliseconds(),
		)

		err := consumer.Ack(consumedMessage.Message)
		if err != nil {
			log.Fatalf("Could not acknowledge message: %v", err)
			return
		}
	}
}
