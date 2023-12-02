package commands

import (
	"fmt"

	"github.com/go-resty/resty/v2"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type SchemaDeleteOptions struct {
	subject string
	version string
}

// AddFlags implements Command.
func (sdOpts *SchemaDeleteOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&sdOpts.subject, "subject", "n", "", "The subject name to delete.")
	if err := cmd.MarkFlagRequired("subject"); err != nil {
		log.Fatalf("Error marking flag 'subject' as required %v", err)
	}
	cmd.Flags().StringVarP(&sdOpts.version, "version", "", "", "The subject version to delete.")
}

// Execute implements Command.
func (sdOpts *SchemaDeleteOptions) Execute(cmd *cobra.Command, args []string) error {
	baseURL := viper.GetString("url")
	url := fmt.Sprintf("%s/subjects/%s", baseURL, sdOpts.subject)
	if sdOpts.version != "" {
		url = url + fmt.Sprintf("/versions/%s", sdOpts.version)
	}

	//Fire the API call
	client := resty.New()
	resp, err := client.R().
		EnableTrace().
		Delete(url)

	if err != nil {
		return err
	} else {
		log.Debugw("Response",
			"Status Code", resp.StatusCode(),
			"Response Body", string(resp.Body()),
		)
		if sdOpts.version != "" {
			log.Infow("Successfully Deleted subject",
				"Subject Name", sdOpts.subject,
				"Version", sdOpts.version)
		} else {
			log.Infow("Successfully Deleted subject",
				"Subject Name", sdOpts.subject,
				"Version", "*")
		}
	}

	return nil
}

// Validate implements Command.
func (sOpts *SchemaDeleteOptions) Validate(cmd *cobra.Command, args []string) error {
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

// SchemaOptions implements Interface
var _ Command = (*SchemaDeleteOptions)(nil)
var schemaDeleteExample = fmt.Sprintf(`
  # Delete schema with default options, deletes all versions of the subject
  %[1]s schema delete --subject foo-value 
  # Delete schema with Schema Registry URL
  %[1]s schema delete --subject foo-value --url http://localhost:18081
  # Delete specific version of the schema
  %[1]s schema delete --subject foo-value --version=1
`, ExamplePrefix())

// NewSchemaDeleteCommand instantiates the new instance of the SchemaDeleteCommand
func NewSchemaDeleteCommand() *cobra.Command {
	schemaDelOpts := &SchemaDeleteOptions{}

	schemaDelCmd := &cobra.Command{
		Use:     "delete",
		Short:   "Delete a Schema by subject from Redpanda Schema Registry.",
		Example: schemaDeleteExample,
		RunE:    schemaDelOpts.Execute,
		PreRunE: schemaDelOpts.Validate,
	}

	schemaDelOpts.AddFlags(schemaDelCmd)

	return schemaDelCmd
}
