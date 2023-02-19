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
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/module/reverse"
	"go.uber.org/zap"
	"strings"
)

type DDL struct {
	SourceSchemaName   string   `json:"source_schema"`
	SourceTableName    string   `json:"source_table_name"`
	SourceTableType    string   `json:"source_table_type"`
	TargetSchemaName   string   `json:"target_schema"`
	TargetTableName    string   `json:"target_table_name"`
	TargetDBType       string   `json:"target_db_type"`
	TargetDBVersion    string   `json:"target_db_version"`
	TablePrefix        string   `json:"table_prefix"`
	TableColumns       []string `json:"table_columns"`
	TableKeys          []string `json:"table_keys"`
	TableSuffix        string   `json:"table_suffix"`
	TableComment       string   `json:"table_comment"`
	TableCheckKeys     []string `json:"table_check_keys""`
	TableForeignKeys   []string `json:"table_foreign_keys"`
	TableCompatibleDDL []string `json:"table_compatible_ddl"`
}

func (d *DDL) Write(w *reverse.Write) error {
	var (
		tableDDL      string
		checkKeyDDL   []string
		foreignKeyDDL []string

		sqlRev  strings.Builder
		sqlComp strings.Builder
	)

	zap.L().Info("reverse oracle table struct", zap.String("table", d.String()))

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

	var reverseDDL string
	if len(d.TableKeys) > 0 {
		reverseDDL = fmt.Sprintf("%s (\n%s,\n%s\n)",
			d.TablePrefix,
			strings.Join(d.TableColumns, ",\n"),
			strings.Join(d.TableKeys, ",\n"))
	} else {
		reverseDDL = fmt.Sprintf("%s (\n%s\n)",
			d.TablePrefix,
			strings.Join(d.TableColumns, ",\n"))
	}

	if strings.EqualFold(d.TableComment, "") {
		tableDDL = fmt.Sprintf("%s %s;", reverseDDL, d.TableSuffix)
	} else {
		tableDDL = fmt.Sprintf("%s %s %s;", reverseDDL, d.TableSuffix, d.TableComment)
	}
	sqlRev.WriteString(tableDDL + "\n\n")

	// foreign and check key sql ddl
	if len(d.TableForeignKeys) > 0 {
		for _, fk := range d.TableForeignKeys {
			fkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;",
				d.TargetSchemaName, d.TargetTableName, fk)
			zap.L().Info("reverse oracle table foreign key",
				zap.String("schema", d.TargetSchemaName),
				zap.String("table", d.TargetTableName),
				zap.String("fk sql", fkSQL))
			foreignKeyDDL = append(foreignKeyDDL, fkSQL)
		}
	}
	if len(d.TableCheckKeys) > 0 {
		for _, ck := range d.TableCheckKeys {
			ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;",
				d.TargetSchemaName, d.TargetTableName, ck)
			zap.L().Info("reverse oracle table check key",
				zap.String("schema", d.TargetSchemaName),
				zap.String("table", d.TargetTableName),
				zap.String("ck sql", ckSQL))
			checkKeyDDL = append(checkKeyDDL, ckSQL)
		}
	}

	// 兼容项处理
	if len(d.TableForeignKeys) > 0 || len(d.TableCheckKeys) > 0 || len(d.TableCompatibleDDL) > 0 {
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
	if d.TargetDBType != common.DatabaseTypeTiDB {
		if len(foreignKeyDDL) > 0 {
			for _, sql := range foreignKeyDDL {
				sqlRev.WriteString(sql + "\n")
			}
		}

		if common.VersionOrdinal(d.TargetDBVersion) > common.VersionOrdinal(common.MySQLCheckConsVersion) {
			if len(checkKeyDDL) > 0 {
				for _, sql := range checkKeyDDL {
					sqlRev.WriteString(sql + "\n")
				}
			}
		} else {
			// 增加不兼容性语句
			if len(checkKeyDDL) > 0 {
				for _, sql := range checkKeyDDL {
					sqlComp.WriteString(sql + "\n")
				}
			}
		}
		// 增加不兼容性语句
		if len(d.TableCompatibleDDL) > 0 {
			for _, sql := range d.TableCompatibleDDL {
				sqlComp.WriteString(sql + "\n")
			}
		}

		// 文件写入
		if sqlRev.String() != "" {
			if w.Cfg.ReverseConfig.DirectWrite {
				if err := w.RWriteDB(sqlRev.String()); err != nil {
					return err
				}
			} else {
				if _, err := w.RWriteFile(sqlRev.String()); err != nil {
					return err
				}
			}
		}
		if sqlComp.String() != "" {
			if _, err := w.CWriteFile(sqlComp.String()); err != nil {
				return err
			}
		}

		return nil
	}

	// TiDB 增加不兼容性语句
	if len(foreignKeyDDL) > 0 {
		for _, sql := range foreignKeyDDL {
			sqlComp.WriteString(sql + "\n")
		}
	}
	if len(checkKeyDDL) > 0 {
		for _, sql := range checkKeyDDL {
			sqlComp.WriteString(sql + "\n")
		}
	}
	if len(d.TableCompatibleDDL) > 0 {
		for _, sql := range d.TableCompatibleDDL {
			sqlComp.WriteString(sql + "\n")
		}
	}
	// 文件写入
	if sqlRev.String() != "" {
		if w.Cfg.ReverseConfig.DirectWrite {
			if err := w.RWriteDB(sqlRev.String()); err != nil {
				return err
			}
		} else {
			if _, err := w.RWriteFile(sqlRev.String()); err != nil {
				return err
			}
		}
	}
	if sqlComp.String() != "" {
		if _, err := w.CWriteFile(sqlComp.String()); err != nil {
			return err
		}
	}
	return nil
}

func (d *DDL) String() string {
	jsonBytes, _ := json.Marshal(d)
	return string(jsonBytes)
}
