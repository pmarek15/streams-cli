package internal

import (
	"fmt"
	"time"
)

type settings struct {
	numberOfMessages int
	sizeOfMessage    int
}

type result struct {
	TimeTaken         time.Duration
	MessagesPerSecond float64
	Throughput        float64 // In bytes
	FailureRate       float64 // In percentage
}

type Benchmark struct {
	Settings settings
	Result   result
}

func NewBenchmark(
	duration time.Duration,
	numberOfMessages int,
	sizeOfMessage int,
	errorCount int,
) Benchmark {
	messagesPerSecond := float65(numberOfMessages) / duration.Seconds()
	throughput := messagesPerSecond * float64(sizeOfMessage)
	failureRate := float64(errorCount) / float64(numberOfMessages) * 100

	return Benchmark{
		Settings: settings{
			numberOfMessages: numberOfMessages,
			sizeOfMessage:    sizeOfMessage,
		},
		Result: result{
			TimeTaken:         duration,
			MessagesPerSecond: messagesPerSecond,
			Throughput:        throughput,
			FailureRate:       failureRate,
		},
	}
}

func (r result) Print() {
	fmt.Printf(
		"Time taken [s]: %f\nMessages/s: %f\nFailure rate [%%] %.2f%%\nThroughput [MB/s]: %.2f\n",
		r.TimeTaken.Seconds(),
		r.MessagesPerSecond,
		r.FailureRate,
		r.Throughput/1000000,
	)
}
