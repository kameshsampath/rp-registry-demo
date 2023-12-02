package commands

import (
	"github.com/spf13/cobra"
)

func NewSchemaCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "schema",
		Short: "Perform various Schema Registry API related operations like upload.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := logSetup(); err != nil {
				return err
			}
			return nil
		},
	}

	rootCmd.AddCommand(NewSchemaSaveCommand())
	rootCmd.AddCommand(NewSchemaDeleteCommand())

	return rootCmd
}
