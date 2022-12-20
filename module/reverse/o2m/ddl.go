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
package o2m

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/module/reverse"
	"strings"
)

type DDL struct {
	SourceSchemaName string   `json:"source_schema"`
	SourceTableName  string   `json:"source_table_name"`
	SourceTableType  string   `json:"source_table_type"`
	TargetSchemaName string   `json:"target_schema"`
	TargetTableName  string   `json:"target_table_name"`
	TargetDBType     string   `json:"target_db_type"`
	TargetDBVersion  string   `json:"target_db_version"`
	ReverseDDL       string   `json:"reverse_ddl"`
	TableSuffix      string   `json:"table_suffix"`
	TableComment     string   `json:"table_comment"`
	CheckKeyDDL      []string `json:"check_key_ddl"`
	ForeignKeyDDL    []string `json:"foreign_key_ddl"`
	CompatibleDDL    []string `json:"compatible_ddl"`
}

func (d *DDL) Writer(f *reverse.File) error {
	var (
		tableDDL string
		sqlRev   strings.Builder
		sqlComp  strings.Builder
	)

	// 表 with 主键
	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(" oracle table reverse sql \n")

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"#", "ORACLE TABLE TYPE", "ORACLE", "MYSQL", "SUGGEST"})
	sw.AppendRows([]table.Row{
		{"TABLE", d.SourceTableType, fmt.Sprintf("%s.%s", d.SourceSchemaName, d.SourceTableName), fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TargetTableName), "Create Table"},
	})
	sqlRev.WriteString(fmt.Sprintf("%v\n", sw.Render()))
	sqlRev.WriteString("*/\n")

	if strings.EqualFold(d.TableComment, "") {
		tableDDL = fmt.Sprintf("%s %s;", d.ReverseDDL, d.TableSuffix)
	} else {
		tableDDL = fmt.Sprintf("%s %s %s;", d.ReverseDDL, d.TableSuffix, d.TableComment)
	}
	sqlRev.WriteString(tableDDL + "\n\n")

	// 兼容项处理
	if len(d.ForeignKeyDDL) > 0 || len(d.CheckKeyDDL) > 0 || len(d.CompatibleDDL) > 0 {
		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" oracle table index or consrtaint maybe mysql has compatibility, skip\n")
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
		tw.AppendRows([]table.Row{
			{"TABLE", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.SourceTableName), fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TargetTableName), "Create Index Or Constraints"}})

		sqlComp.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		sqlComp.WriteString("*/\n")
	}

	// 外键约束、检查约束
	if d.TargetDBType != common.TaskDBTiDB {
		if len(d.ForeignKeyDDL) > 0 {
			for _, sql := range d.ForeignKeyDDL {
				sqlRev.WriteString(sql + "\n")
			}
		}

		if common.VersionOrdinal(d.TargetDBVersion) > common.VersionOrdinal(common.MySQLCheckConsVersion) {
			if len(d.CheckKeyDDL) > 0 {
				for _, sql := range d.CheckKeyDDL {
					sqlRev.WriteString(sql + "\n")
				}
			}
		} else {
			// 增加不兼容性语句
			if len(d.CheckKeyDDL) > 0 {
				for _, sql := range d.CheckKeyDDL {
					sqlComp.WriteString(sql + "\n")
				}
			}
		}
		// 增加不兼容性语句
		if len(d.CompatibleDDL) > 0 {
			for _, sql := range d.CompatibleDDL {
				sqlComp.WriteString(sql + "\n")
			}
		}

		// 文件写入
		if sqlRev.String() != "" {
			if _, err := f.RWriteString(sqlRev.String()); err != nil {
				return err
			}
		}
		if sqlComp.String() != "" {
			if _, err := f.CWriteString(sqlComp.String()); err != nil {
				return err
			}
		}

		return nil
	}

	// TiDB 增加不兼容性语句
	if len(d.ForeignKeyDDL) > 0 {
		for _, sql := range d.ForeignKeyDDL {
			sqlComp.WriteString(sql + "\n")
		}
	}
	if len(d.CheckKeyDDL) > 0 {
		for _, sql := range d.CheckKeyDDL {
			sqlComp.WriteString(sql + "\n")
		}
	}
	if len(d.CompatibleDDL) > 0 {
		for _, sql := range d.CompatibleDDL {
			sqlComp.WriteString(sql + "\n")
		}
	}
	// 文件写入
	if sqlRev.String() != "" {
		if _, err := f.RWriteString(sqlRev.String()); err != nil {
			return err
		}
	}
	if sqlComp.String() != "" {
		if _, err := f.CWriteString(sqlComp.String()); err != nil {
			return err
		}
	}
	return nil
}
