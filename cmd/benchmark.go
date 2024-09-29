package cmd

import (
	"stream/internal"
	"stream/internal/kafka"
	"stream/internal/pulsar"

	"github.com/spf13/cobra"
)

var (
	numberOfMessages int
	sizeOfMessage    int
	benchmarkCmd     = &cobra.Command{
		Use:   "benchmark",
		Short: "Data streaming technology benchmark",
		Long:  ``,
		Run: func(_ *cobra.Command, _ []string) {
			var benchmark internal.Benchmark

			switch target {
			case targetEnumKafka:
				benchmark = kafka.Benchmark(
					config.KafkaConfig,
					numberOfMessages,
					sizeOfMessage,
				)
			case targetEnumPulsar:
				benchmark = pulsar.Benchmark(
					config.PulsarConfig,
					numberOfMessages,
					sizeOfMessage,
				)
			case targetEnumRedpanda:
				benchmark = kafka.Benchmark(
					config.RedpandaConfig,
					numberOfMessages,
					sizeOfMessage,
				)
			}

			benchmark.Result.Print()
		},
	}
)

func init() {
	rootCmd.AddCommand(benchmarkCmd)

	benchmarkCmd.Flags().IntVarP(
		&numberOfMessages,
		"number",
		"n",
		100,
		"Number of messages to be send",
	)
	benchmarkCmd.Flags().
		IntVarP(&sizeOfMessage,
			"size",
			"s",
			512,
			"Size of the message to be send",
		)
}
