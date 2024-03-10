/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"stream/internal"

	"github.com/spf13/cobra"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Command to create consumer",
	Long: ``,

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("consume called")

        internal.ConsumeKafka(config.KafkaConfig) //TODO finish implementation for other brokers
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)
}
