package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"math/rand"
	"stream/internal"
	"time"
)

func Produce(pulsarConfig internal.PulsarConfig, frequency int, max int) {
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
			&internal.Message{Number: randNumber, CreatedAt: time.Now()},
		)

		msg := pulsar.ProducerMessage{
			Payload: jsonMessage,
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
