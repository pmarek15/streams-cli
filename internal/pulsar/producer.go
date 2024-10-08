package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"stream/internal"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type message struct {
	Number    float64   `json:"number"`
	CreatedAt time.Time `json:"created_at"`
}

func Produce(pulsarConfig internal.PulsarConfig, frequency int, max int) {
	client := GetClient(pulsarConfig)

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
			&message{Number: randNumber, CreatedAt: time.Now()},
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
