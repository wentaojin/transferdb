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
package m2o

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/module/reverse"
	"strings"
)

type DDL struct {
	SourceSchemaName string   `json:"source_schema"`
	SourceTableName  string   `json:"source_table_name"`
	IsPartition      bool     `json:"is_partition"`
	SourceTableType  string   `json:"source_table_type"`
	TargetSchemaName string   `json:"target_schema"`
	TargetTableName  string   `json:"target_table_name"`
	ReverseDDL       string   `json:"reverse_ddl"`
	ColumnCommentDDL []string `json:"column_comment_ddl"`
	TableComment     string   `json:"table_comment"`
	TableIndexDDL    []string `json:"table_index_ddl"`
	CheckKeyDDL      []string `json:"check_key_ddl"`
	ForeignKeyDDL    []string `json:"foreign_key_ddl"`
	CompatibleDDL    []string `json:"compatible_ddl"`
}

func (d *DDL) Writer(f *reverse.File) error {
	var (
		sqlRev  strings.Builder
		sqlComp strings.Builder
	)

	// 表 with 主键
	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(" mysql table reverse sql \n")

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"#", "TABLE TYPE", "MySQL", "ORACLE", "SUGGEST"})
	if d.IsPartition {
		sw.AppendRows([]table.Row{
			{"TABLE", "PARTITION", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.SourceTableName), fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TargetTableName), "Create Table"},
		})
	} else {
		sw.AppendRows([]table.Row{
			{"TABLE", "NORMAL", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.SourceTableName), fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TargetTableName), "Create Table"},
		})
	}

	sqlRev.WriteString(fmt.Sprintf("%v\n", sw.Render()))
	sqlRev.WriteString("*/\n")

	sqlRev.WriteString(d.ReverseDDL + "\n")

	if len(d.ColumnCommentDDL) > 0 {
		sqlRev.WriteString(strings.Join(d.ColumnCommentDDL, "\n") + "\n")
	}
	if d.TableComment != "" {
		sqlRev.WriteString(d.TableComment + "\n")

	}
	if len(d.CheckKeyDDL) > 0 {
		sqlRev.WriteString(strings.Join(d.CheckKeyDDL, "\n") + "\n")
	}

	if len(d.ForeignKeyDDL) > 0 {
		sqlRev.WriteString(strings.Join(d.ForeignKeyDDL, "\n") + "\n")
	}

	if len(d.TableIndexDDL) > 0 {
		sqlRev.WriteString(strings.Join(d.TableIndexDDL, "\n") + "\n")
	}

	if len(d.CompatibleDDL) > 0 {
		sqlComp.WriteString(strings.Join(d.CompatibleDDL, "\n") + "\n")
	}

	// 文件写入
	if sqlRev.String() != "" {
		if _, err := f.CWriteString(sqlRev.String()); err != nil {
			return err
		}
	}
	if sqlComp.String() != "" {
		if _, err := f.RWriteString(sqlComp.String()); err != nil {
			return err
		}
	}
	return nil
}
