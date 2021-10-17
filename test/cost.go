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
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/jedib0t/go-pretty/v6/table"
)

func main() {
	pwdDir, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
	}
	file, err := os.OpenFile(filepath.Join(pwdDir, "transferdb_cost.txt"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	t := table.NewWriter()
	t.SetOutputMirror(file)
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"David", "David", "David", "David", "David", "David"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Node IP", "Pods", "Namespace", "Container", "RCE", "RCE"})
	t.AppendRow(table.Row{"1.1.1.1", "Pod 1A", "NS 1A", "C 1", "Y", "Y"})
	t.AppendRow(table.Row{"1.1.1.1", "Pod 1A", "NS 1A", "C 2", "Y", "N"})
	t.AppendRow(table.Row{"1.1.1.1", "Pod 1A", "NS 1B", "C 3", "N", "N"})
	t.AppendRow(table.Row{"1.1.1.1", "Pod 1B", "NS 2", "C 4", "N", "N"})
	t.AppendRow(table.Row{"1.1.1.2", "Pod 1B", "NS 2", "C 5", "Y", "N"})
	t.AppendRow(table.Row{"2.2.2.2", "Pod 2", "NS 3", "C 6", "Y", "Y"})
	t.AppendRow(table.Row{"2.2.2.3", "Pod 2", "NS 3", "C 7", "Y", "Y"})
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.Render()
}
