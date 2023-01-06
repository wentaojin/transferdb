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
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/module/reverse"
	"go.uber.org/zap"
	"strings"
)

type DDL struct {
	SourceSchemaName     string   `json:"source_schema"`
	SourceTableName      string   `json:"source_table_name"`
	SourceTableType      string   `json:"source_table_type"`
	TargetSchemaName     string   `json:"target_schema"`
	TargetTableName      string   `json:"target_table_name"`
	TablePrefix          string   `json:"table_prefix"`
	TableColumns         []string `json:"table_columns"`
	TableKeys            []string `json:"table_keys"`
	TableIndexes         []string `json:"table_indexes"`
	TableSuffix          string   `json:"table_suffix"`
	TableComment         string   `json:"table_comment"`
	ColumnCommentDDL     []string `json:"column_comment_ddl"`
	TableCheckKeys       []string `json:"table_check_keys""`
	TableForeignKeys     []string `json:"table_foreign_keys"`
	TableCompatibleDDL   []string `json:"table_compatible_ddl"`
	TablePartitionDetail string   `json:"table_partition_detail"`
}

func (d *DDL) File(f *reverse.File) error {
	var (
		checkKeyDDL   []string
		foreignKeyDDL []string
		sqlRev        strings.Builder
		sqlComp       strings.Builder
	)
	zap.L().Info("reverse mysql table struct", zap.String("table", d.String()))

	// 表 with 主键
	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(" mysql table reverse sql \n")

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"#", "TABLE TYPE", "MySQL", "ORACLE", "SUGGEST"})
	if !strings.EqualFold(d.TablePartitionDetail, "") {
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

	// table create sql -> target
	var reverseDDL string
	if strings.EqualFold(d.TablePartitionDetail, "") {
		if len(d.TableKeys) > 0 {
			reverseDDL = fmt.Sprintf("%s (\n%s,\n%s\n);",
				d.TablePrefix,
				strings.Join(d.TableColumns, ",\n"),
				strings.Join(d.TableKeys, ",\n"))
		} else {
			reverseDDL = fmt.Sprintf("%s (\n%s\n);",
				d.TablePrefix,
				strings.Join(d.TableColumns, ",\n"))
		}
	} else {
		if len(d.TableKeys) > 0 {
			reverseDDL = fmt.Sprintf("%s (\n%s,\n%s\n) PARTITION BY %s;",
				d.TablePrefix,
				strings.Join(d.TableColumns, ",\n"),
				strings.Join(d.TableKeys, ",\n"),
				d.TablePartitionDetail)
		} else {
			reverseDDL = fmt.Sprintf("%s (\n%s\n) PARTITION BY %s;",
				d.TablePrefix,
				strings.Join(d.TableColumns, ",\n"),
				d.TablePartitionDetail)
		}
	}

	sqlRev.WriteString(reverseDDL + "\n")

	if len(d.ColumnCommentDDL) > 0 {
		sqlRev.WriteString(strings.Join(d.ColumnCommentDDL, "\n") + "\n")
	}
	if d.TableComment != "" {
		sqlRev.WriteString(d.TableComment + "\n")
	}

	if len(d.TableCheckKeys) > 0 {
		for _, ck := range d.TableCheckKeys {
			ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", d.TargetSchemaName, d.TargetTableName, ck)
			zap.L().Info("reverse",
				zap.String("schema", d.TargetSchemaName),
				zap.String("table", d.TargetTableName),
				zap.String("ck sql", ckSQL))

			checkKeyDDL = append(checkKeyDDL, ckSQL)
		}
		sqlRev.WriteString(strings.Join(checkKeyDDL, "\n") + "\n")
	}

	if len(d.TableForeignKeys) > 0 {
		for _, fk := range d.TableForeignKeys {
			addFkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", d.TargetSchemaName, d.TargetTableName, fk)
			zap.L().Info("reverse",
				zap.String("schema", d.TargetSchemaName),
				zap.String("table", d.TargetTableName),
				zap.String("fk sql", addFkSQL))
			foreignKeyDDL = append(foreignKeyDDL, addFkSQL)
		}
		sqlRev.WriteString(strings.Join(foreignKeyDDL, "\n") + "\n")
	}

	if len(d.TableIndexes) > 0 {
		sqlRev.WriteString(strings.Join(d.TableIndexes, "\n") + "\n")
	}

	if len(d.TableCompatibleDDL) > 0 {
		sqlComp.WriteString(strings.Join(d.TableCompatibleDDL, "\n") + "\n")
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

func (d *DDL) String() string {
	jsonBytes, _ := json.Marshal(d)
	return string(jsonBytes)
}
