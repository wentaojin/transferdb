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
	"github.com/spf13/cobra"
)

// App is storage for app's root command
type App struct {
	Host         string
	Port         string
	User         string
	Password     string
	DBName       string
	Concurrency  int // app run concurrency
	IncludeTable []string
	ExcludeTable []string
	All          bool
	RegexTable   string
	Version      string   // transferdb app version
	Args         []string // Args set by sub commands
}

// Cmd returns a cobra.Command instance to be added to app's root command
func (app *App) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "transferdb",
		Short:             "CLI transferdb app for tidb cluster",
		PersistentPreRunE: app.PersistentPreRunE,
		SilenceUsage:      true,
	}
	return cmd
}

// PersistentPreRunE is a global initializer for this app
func (app *App) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	return nil
}
