/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"errors"
	"fmt"

    "stream/internal"

	"github.com/spf13/cobra"
)


type targetEnum string
const (
    targetEnumKafka targetEnum = "kafka"
    targetEnumPulsar targetEnum = "pulsar"
    targetEnumRedpanda targetEnum = "redpanda"
)

func (e *targetEnum) String() string {
    return string(*e)
}

func (e *targetEnum) Set(v string) error {
    switch v {
        case "kafka", "pulsar", "redpanda":
            *e = targetEnum(v)
            return nil
    default:
        return errors.New(`must be one of "kafka", "pulsar", "redpanda"`)
    }
}

func (e *targetEnum) Type() string {
    return "targetEnum"
}


// benchmarkCmd represents the benchmark command
var(
    target targetEnum
    numberOfMessages int
    sizeOfMessage int
    benchmarkCmd = &cobra.Command{
        Use:   "benchmark",
        Short: "Data streaming technology benchmark",
        Long: ``,
        Run: func(cmd *cobra.Command, args []string) {
            switch target {
                case targetEnumKafka:
                    internal.Connect(Config.KafkaConfig.BootstrapServers, numberOfMessages, sizeOfMessage)
                default:
                    fmt.Println("Default")
            }
        },
    }
)

func init() {
	rootCmd.AddCommand(benchmarkCmd)

    benchmarkCmd.Flags().VarP(&target, "target", "t", "Target for benchmark [kafka, redpanda, pulsar]")
    benchmarkCmd.MarkFlagRequired("target")

    benchmarkCmd.Flags().IntVarP(&numberOfMessages, "number", "n", 100, "Number of messages to be send")
    benchmarkCmd.Flags().IntVarP(&sizeOfMessage, "size", "s", 512, "Size of the message to be send")
}
