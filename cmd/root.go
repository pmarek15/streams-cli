/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type kafkaConfig struct {
    BootstrapServers string `yaml:"bootstrapServers"`
}


type redpandaConfig struct {
    BootstrapServers string `yaml:"bootstrapServers"`
}

type config struct {
    KafkaConfig kafkaConfig `yaml:"kafka"`
    RedpandaConfig redpandaConfig `yaml:"redpanda"`
}

var (
    cfgFile string
    rootCmd = &cobra.Command{
            Use:   "stream",
            Short: "CLI app for testing streaming technologies",
            Long: "",
            Run: func(cmd *cobra.Command, args []string) { },
    }
)

var Config config

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
        fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
    cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
}

func initConfig() {
    if cfgFile == "" {
        cfgFile = "config.yaml"
    }

    config, err := parseConfig(cfgFile)
	if err != nil {
        fmt.Fprintln(os.Stderr, err)
        return
    }

    // Store parsed config into a var
    Config = config
}

func parseConfig(file string) (config, error) {
    rawData, err := os.ReadFile(file)

    if err != nil {
        return config{}, err
    }

    var configData config

    err = yaml.Unmarshal(rawData, &configData)

    if err != nil {
        return config{}, err
    }

    return configData, nil
}
