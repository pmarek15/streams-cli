package cmd

import (
	"fmt"
	"stream/internal/kafka"
	"stream/internal/pulsar"

	"github.com/spf13/cobra"
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Command to create consumer",
	Long:  ``,

	Run: func(_ *cobra.Command, _ []string) {
		fmt.Println("consume called")

		switch target {
		case targetEnumKafka:
			kafka.Consume(config.KafkaConfig, frequency)
		case targetEnumPulsar:
			pulsar.Consume(config.PulsarConfig)
		case targetEnumRedpanda:
			kafka.Consume(config.RedpandaConfig, frequency)
		}
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().IntVarP(
		&frequency,
		"frequency",
		"f",
		1000,
		"Frequency of producing the messages in milliseconds",
	)
	consumeCmd.MarkFlagRequired("frequency")
}
