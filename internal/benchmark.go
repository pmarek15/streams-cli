package internal

import (
	"fmt"
	"time"
)

type settings struct {
    numberOfMessages int
    sizeOfMessage int
}

type result struct {
    TimeTaken time.Duration
    MessagesPerSecond float64
    Throughput float64 // In bytes
}

type Benchmark struct {
    Settings settings
    Result result
}

func NewBenchmark(duration time.Duration, numberOfMessages int, sizeOfMessage int) Benchmark {
    messagesPerSecond := float64(numberOfMessages) / duration.Seconds()
    throughput := messagesPerSecond * float64(sizeOfMessage)

    return Benchmark{
        Settings: settings{
            numberOfMessages: numberOfMessages,
            sizeOfMessage: sizeOfMessage,
        },
        Result: result{
            TimeTaken: duration,
            MessagesPerSecond: messagesPerSecond,
            Throughput: throughput,
        },
    }
}

func (r result) Print() {
    fmt.Printf(
        "Time taken [s]: %f\nMessages/s: %f\nThroughput [MB/s]: %.2f\n",
        r.TimeTaken.Seconds(),
        r.MessagesPerSecond,
        r.Throughput / 1000000,
    )
}
