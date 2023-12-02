package commands

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var v string
var log *zap.SugaredLogger

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "rp-registry-demo",
		Short: "A demo to show how to use Schema Registry with Redpanda.",
		Long: `The demo uses protobuf as the Schema for Kafka records, showing how to add Schema to registry and use it when producing and consuming messages.
    `,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := logSetup(); err != nil {
				return err
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(&v, "verbose", "v", zap.InfoLevel.String(), "The logging level to set")
	rootCmd.PersistentFlags().StringP("url", "l", "http://localhost:18081", "The Redpanda Schema Registry API URL.")

	rootCmd.AddCommand(NewSchemaCommand())
	rootCmd.AddCommand(NewProducerCommand())
	rootCmd.AddCommand(NewConsumerCommand())

	return rootCmd
}

// TODO improve zap configuration
func logSetup() error {
	jsonConfig := []byte(fmt.Sprintf(`{
"level": "%s",
"encoding": "json",
"outputPaths": ["stdout"],
"errorOutputPaths": ["stderr"],
"encoderConfig": {
  "messageKey": "message",
  "levelEncoder": "lowercase"
}
}
`, v))
	var cfg zap.Config
	if err := json.Unmarshal(jsonConfig, &cfg); err != nil {
		panic(err)
	}
	logger := zap.Must(cfg.Build())
	defer logger.Sync()

	logger.Debug("logger construction succeeded")
	log = logger.Sugar()
	return nil
}
