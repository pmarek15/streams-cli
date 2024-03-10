/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"errors"
	"log"
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


var (
    cfgFile string
    target targetEnum
    config internal.Config
    rootCmd = &cobra.Command{
            Use:   "stream",
            Short: "CLI app for testing streaming technologies",
            Long: "",
            Run: func(cmd *cobra.Command, args []string) { },
    }
)

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
        log.Fatal(err)
	}
}

func init() {
    cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")

    benchmarkCmd.Flags().VarP(&target, "target", "t", "Target for benchmark [kafka, redpanda, pulsar]")
    benchmarkCmd.MarkFlagRequired("target")
}

func initConfig() {
    if cfgFile == "" {
        cfgFile = "config.yaml"
    }

    cfg, err := internal.ParseConfig(cfgFile)
	if err != nil {
        log.Fatal(err)
    }

    config = cfg
}
