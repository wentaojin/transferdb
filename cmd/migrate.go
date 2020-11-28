/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"github.com/WentaoJin/transferdb/pkg/migrate"
	"github.com/spf13/cobra"
)

// AppSplit is storage for the sub command analyze
// includeTable、excludeTable、regexTable only one of the three
type AppMigrate struct {
	*App   // embedded parent command storage
	Config string
}

func (app *App) AppMigrate() Cmder {
	return &AppMigrate{App: app}
}

func (app *AppMigrate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "migrate",
		Short:        "Migrate oracle schema to mysql",
		Long:         `Migrate oracle schema to mysql`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVar(&app.Config, "config", "", "configure migrate toml file")

	return cmd
}

func (app *AppMigrate) RunE(cmd *cobra.Command, args []string) error {
	if app.Config == "" {
		if err := cmd.Help(); err != nil {
			return err
		}
	}
	cfg, err := migrate.NewConvertCmdCfg(app.Config)
	if err != nil {
		return err
	}
	migrate.ConvertOracleCreateTableSQL(cfg)

	return nil
}
