/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"stream/internal"

	"github.com/spf13/cobra"
)

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Command to create a producer",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("produce called")

        internal.ProduceKafka(config.KafkaConfig, 10) //TODO Finish implementation for other brokers
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

}
