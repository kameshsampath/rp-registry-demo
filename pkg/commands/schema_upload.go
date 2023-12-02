package commands

import (
	"fmt"
	"os"

	"github.com/go-resty/resty/v2"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type SchemaSaveOptions struct {
	schemaFile string
	topic      string
	isKey      bool
}

// AddFlags implements Command.
func (sOpts *SchemaSaveOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sOpts.topic, "topic", "t", "", "The topic to use with the subject.")
	if err := cmd.MarkFlagRequired("topic"); err != nil {
		log.Fatalf("Error marking flag 'topic' as required %v", err)
	}
	cmd.Flags().StringVarP(&sOpts.schemaFile, "schema-file", "f", "", "The absolute path to schema file.")
	if err := cmd.MarkFlagRequired("schema-file"); err != nil {
		log.Fatalf("Error marking flag 'schema-file' as required %v", err)
	}
	cmd.Flags().BoolVarP(&sOpts.isKey, "for-key", "k", false, "Schema subject is registered for record key. Default is registered for record value.")
}

// Execute implements Command.
func (sOpts *SchemaSaveOptions) Execute(cmd *cobra.Command, args []string) error {
	baseURL := viper.GetString("url")
	in, err := os.ReadFile(sOpts.schemaFile)
	if err != nil {
		return err
	}
	//Build the request body
	var schema = map[string]string{
		"schemaType": "PROTOBUF",
		"schema":     string(in),
	}

	url := fmt.Sprintf("%s/subjects/%s-value/versions", baseURL, sOpts.topic)
	if sOpts.isKey {
		url = fmt.Sprintf("%s/subjects/%s-key/versions", baseURL, sOpts.topic)
	}

	client := resty.New()
	resp, err := client.R().
		EnableTrace().
		SetHeader("Content-Type", "application/json").
		SetBody(schema).
		Post(url)

	if err != nil {
		return err
	} else {
		log.Debugw("Response",
			"Status Code", resp.StatusCode(),
			"Response Body", string(resp.Body()),
		)
		log.Infow("Saved schema with ID",
			"Schema ID:",
			string(resp.Body()),
		)
	}

	return nil
}

// Validate implements Command.
func (sOpts *SchemaSaveOptions) Validate(cmd *cobra.Command, args []string) error {
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

// SchemaSaveOptions implements Interface
var _ Command = (*SchemaSaveOptions)(nil)
var schemaSaveExample = fmt.Sprintf(`
  # Save schema with default options
  %[1]s schema save --schema-file foo.proto --topic greetings
  # Save schema with Redpanda Schema Registry API URL
  %[1]s schema save --schema-file foo.proto --topic greetings --url http://localhost:18081
  # Save schema with Redpanda Schema Registry with the subject named after topic with key e.g. greetings-key. Default is for "value".
  %[1]s schema save --schema-file foo.proto --topic greetings --for-key
`, ExamplePrefix())

// NewSchemaSaveCommand instantiates the new instance of the StartCommand
func NewSchemaSaveCommand() *cobra.Command {
	schemaSaveOpts := &SchemaSaveOptions{}

	schemaSaveCmd := &cobra.Command{
		Use:     "save",
		Short:   "Save Schema to Redpanda Schema Registry.",
		Example: schemaSaveExample,
		RunE:    schemaSaveOpts.Execute,
		PreRunE: schemaSaveOpts.Validate,
	}

	schemaSaveOpts.AddFlags(schemaSaveCmd)

	return schemaSaveCmd
}
