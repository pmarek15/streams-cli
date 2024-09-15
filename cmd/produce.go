package cmd

import (
	"fmt"
	"stream/internal/kafka"
	"stream/internal/pulsar"

	"github.com/spf13/cobra"
)

var frequency int
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Command to create a producer",
	Long:  ``,
	Run: func(_ *cobra.Command, _ []string) {
		fmt.Println("produce called")

		switch target {
		case targetEnumKafka:
			kafka.Produce(config.KafkaConfig, frequency, 10)
		case targetEnumPulsar:
			pulsar.Produce(config.PulsarConfig, frequency, 10)
			//connector.ProducePulsar(config.PulsarConfig, frequency, 10)
		case targetEnumRedpanda:
			kafka.Produce(config.RedpandaConfig, frequency, 10)
		}
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().IntVarP(
		&frequency,
		"frequency",
		"f",
		1000,
		"Frequency of producing the messages in milliseconds",
	)
	produceCmd.MarkFlagRequired("frequency")
}
