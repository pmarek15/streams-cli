package internal

import "github.com/apache/pulsar-client-go/pulsar"

func ConnectPulsar(url string, numberOfMessages int, sizeOfMessage int) {
    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: url,
    })

    if err != nil {
        panic(err)
    }


    defer client.Close()
}
