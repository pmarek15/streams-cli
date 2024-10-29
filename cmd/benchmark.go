package cmd

import (
	"stream/internal"
	"stream/internal/kafka"
	"stream/internal/pulsar"

	"github.com/spf13/cobra"
)

var (
	duration      int
	sizeOfMessage int
	benchmarkCmd  = &cobra.Command{
		Use:   "benchmark",
		Short: "Data streaming technology benchmark",
		Long:  ``,
		Run: func(_ *cobra.Command, _ []string) {
			var benchmark internal.Benchmark

			switch target {
			case targetEnumKafka:
				benchmark = kafka.Benchmark(
					config.KafkaConfig,
					duration,
					sizeOfMessage,
				)
			case targetEnumPulsar:
				benchmark = pulsar.Benchmark(
					config.PulsarConfig,
					duration,
					sizeOfMessage,
				)
			case targetEnumRedpanda:
				benchmark = kafka.Benchmark(
					config.RedpandaConfig,
					duration,
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
		&duration,
		"duration",
		"d",
		30,
		"Duration of the benchmark in seconds",
	)
	benchmarkCmd.Flags().
		IntVarP(&sizeOfMessage,
			"size",
			"s",
			512,
			"Size of the message to be send",
		)
}
