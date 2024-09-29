package pulsar

import (
	"log"
	"stream/internal"

	"github.com/apache/pulsar-client-go/pulsar"
)

// GetClient creates and returns a new Pulsar client based on the provided configuration
func GetClient(pulsarConfig internal.PulsarConfig) pulsar.Client {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarConfig.Url,
	})

	if err != nil {
		log.Fatal(err)
	}

	return client
}
