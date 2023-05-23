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
	SourceTableDDL       string   `json:"-"` // 忽略
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

func (d *DDL) Write(w *reverse.Write) (string, error) {
	if w.Cfg.ReverseConfig.DirectWrite {
		errSql, err := d.WriteDB(w)
		if err != nil {
			return errSql, err
		}
	} else {
		errSql, err := d.WriteFile(w)
		if err != nil {
			return errSql, err
		}
	}
	return "", nil
}

func (d *DDL) WriteFile(w *reverse.Write) (string, error) {
	var (
		sqlRev  strings.Builder
		sqlComp strings.Builder
	)
	revDDLS, compDDLS := d.GenDDLStructure()

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
	sqlRev.WriteString(fmt.Sprintf("ORIGIN DDL:%v\n", d.SourceTableDDL))
	sqlRev.WriteString("*/\n")

	sqlRev.WriteString(strings.Join(revDDLS, "\n"))

	// 兼容项处理
	if len(compDDLS) > 0 {
		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" mysql table structure object maybe oracle has compatibility, skip\n")
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"#", "MYSQL", "ORACLE", "SUGGEST"})
		tw.AppendRows([]table.Row{
			{"TABLE", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.SourceTableName), fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TargetTableName), "Manual Processing"}})

		sqlComp.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		sqlComp.WriteString("*/\n")

		sqlComp.WriteString(strings.Join(compDDLS, "\n"))
	}

	// 数据写入
	if sqlRev.String() != "" {
		if _, err := w.RWriteFile(sqlRev.String()); err != nil {
			return sqlRev.String(), err
		}
	}
	if sqlComp.String() != "" {
		if _, err := w.CWriteFile(sqlComp.String()); err != nil {
			return sqlComp.String(), err
		}
	}
	return "", nil
}

func (d *DDL) WriteDB(w *reverse.Write) (string, error) {
	var (
		sqlRev  strings.Builder
		sqlComp strings.Builder
	)
	revDDLS, compDDLS := d.GenDDLStructure()

	// 表 with 主键
	sqlRev.WriteString(strings.Join(revDDLS, "\n"))

	// 兼容项处理
	if len(compDDLS) > 0 {
		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" mysql table structure object maybe oracle has compatibility, skip\n")
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"#", "MYSQL", "ORACLE", "SUGGEST"})
		tw.AppendRows([]table.Row{
			{"TABLE", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.SourceTableName), fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TargetTableName), "Manual Processing"}})

		sqlComp.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		sqlComp.WriteString("*/\n")

		sqlComp.WriteString(strings.Join(compDDLS, "\n"))
	}

	// 数据写入
	if sqlRev.String() != "" {
		if err := w.RWriteDB(sqlRev.String()); err != nil {
			return sqlRev.String(), err
		}
	}
	if sqlComp.String() != "" {
		if _, err := w.CWriteFile(sqlComp.String()); err != nil {
			return sqlComp.String(), err
		}
	}
	return "", nil
}

func (d *DDL) GenDDLStructure() ([]string, []string) {
	var (
		revDDLS       []string
		compDDLS      []string
		checkKeyDDL   []string
		foreignKeyDDL []string
	)

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

	zap.L().Info("reverse mysql table structure",
		zap.String("schema", d.TargetSchemaName),
		zap.String("table", d.TargetTableName),
		zap.String("sql", reverseDDL))

	revDDLS = append(revDDLS, reverseDDL+"\n")

	if len(d.ColumnCommentDDL) > 0 {
		revDDLS = append(revDDLS, strings.Join(d.ColumnCommentDDL, "\n"))
	}
	if d.TableComment != "" {
		revDDLS = append(revDDLS, d.TableComment+"\n")
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
		revDDLS = append(revDDLS, strings.Join(checkKeyDDL, "\n"))
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
		revDDLS = append(revDDLS, strings.Join(foreignKeyDDL, "\n"))
	}

	if len(d.TableIndexes) > 0 {
		revDDLS = append(revDDLS, strings.Join(d.TableIndexes, "\n"))
	}

	if len(d.TableCompatibleDDL) > 0 {
		compDDLS = append(compDDLS, strings.Join(d.TableCompatibleDDL, "\n"))
	}

	return revDDLS, compDDLS
}

func (d *DDL) String() string {
	jsonBytes, _ := json.Marshal(d)
	return string(jsonBytes)
}
