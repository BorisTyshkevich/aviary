package cmd

import (
	"github.com/spf13/cobra"

	"github.com/lsegal/aviary/internal/chlab"
)

func init() {
	rootCmd.AddCommand(&cobra.Command{
		Use:    "chlab-service",
		Short:  "Run the root-owned ClickHouse lab service",
		Hidden: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			listener, err := chlab.Listen()
			if err != nil {
				return err
			}
			defer func() { _ = listener.Close() }()
			return chlab.NewService().Serve(listener)
		},
	})
}
