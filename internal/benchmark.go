package internal

import (
	"fmt"
	"time"
)

type settings struct {
	duration      int
	sizeOfMessage int
}

type result struct {
	TimeTaken         time.Duration
	MessagesPerSecond float64
	Throughput        float64 // In bytes
	FailureRate       float64 // In percentage
	TotalMessages     int     // Total messages sent during the duration
}

type Benchmark struct {
	Settings settings
	Result   result
}

func NewBenchmark(
	duration time.Duration,
	totalMessages int,
	sizeOfMessage int,
	errorCount int,
) Benchmark {
	messagesPerSecond := float64(totalMessages) / duration.Seconds()
	throughput := messagesPerSecond * float64(sizeOfMessage)
	failureRate := 0.0
	if totalMessages > 0 {
		failureRate = float64(errorCount) / float64(totalMessages) * 100
	}

	return Benchmark{
		Settings: settings{
			duration:      int(duration.Seconds()),
			sizeOfMessage: sizeOfMessage,
		},
		Result: result{
			TimeTaken:         duration,
			MessagesPerSecond: messagesPerSecond,
			Throughput:        throughput,
			FailureRate:       failureRate,
			TotalMessages:     totalMessages,
		},
	}
}

func (r result) Print() {
	fmt.Printf(
		"Time taken [s]: %.2f\n"+
			"Total messages: %d\n"+
			"Messages/s: %.2f\n"+
			"Failure rate [%%]: %.2f%%\n"+
			"Throughput [MB/s]: %.2f\n",
		r.TimeTaken.Seconds(),
		r.TotalMessages,
		r.MessagesPerSecond,
		r.FailureRate,
		r.Throughput/1000000,
	)
}
