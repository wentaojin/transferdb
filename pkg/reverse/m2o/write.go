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
	"io"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/pkg/reverse"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

type ReverseWriter struct {
	DBVersion           string
	CreateTable         string
	CreateIndex         []string
	CreateTableComment  string
	CreateColumnComment []string
	ReverseTable        Table
	RevFileMW           *FileMW `json:"-"`
	CompFileMW          *FileMW `json:"-"`
}

type FileMW struct {
	Mutex  sync.Mutex
	Writer io.Writer
}

func (d *FileMW) Write(b []byte) (n int, err error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	return d.Writer.Write(b)
}

func NewReverseWriter(t Table, revFileMW, compFileMW *FileMW) (*ReverseWriter, error) {
	// 表名转换
	modifyTableName := reverse.ChangeOracleTableName(t.SourceTableName, t.TargetTableName)
	t.TargetTableName = modifyTableName

	// 表结构
	createTableSQL, createColumnCommentSQL, err := t.GenCreateTableSQL()
	if err != nil {
		return nil, err
	}

	// 索引（唯一/非唯一）
	createIndexSQL, err := t.GenCreateIndexSQL()
	if err != nil {
		return nil, fmt.Errorf("mysql db reverse table [%s] normal and unique index failed: %v", t.TargetTableName, err)
	}

	// 表注释
	createTableCommentSQL, err := t.GenCreateTableCommentSQL()
	if err != nil {
		return nil, fmt.Errorf("mysql db reverse table [%s] comments failed: %v", t.TargetTableName, err)
	}

	return &ReverseWriter{
		DBVersion:           t.OracleDBVersion,
		CreateTable:         createTableSQL,
		CreateTableComment:  createTableCommentSQL,
		CreateColumnComment: createColumnCommentSQL,
		CreateIndex:         createIndexSQL,
		ReverseTable:        t,
		RevFileMW:           revFileMW,
		CompFileMW:          compFileMW,
	}, nil

}

func (d *ReverseWriter) Reverse() error {
	var (
		sqlRev strings.Builder
	)

	// 表 with 主键
	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(" mysql table reverse sql \n")

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"#", "TABLE TYPE", "MySQL", "ORACLE", "SUGGEST"})
	if d.ReverseTable.IsPartition {
		sw.AppendRows([]table.Row{
			{"TABLE", "PARTITION", fmt.Sprintf("%s.%s", d.ReverseTable.SourceSchemaName, d.ReverseTable.SourceTableName), fmt.Sprintf("%s.%s", d.ReverseTable.TargetSchemaName, d.ReverseTable.TargetTableName), "Create Table"},
		})
	} else {
		sw.AppendRows([]table.Row{
			{"TABLE", "NORMAL", fmt.Sprintf("%s.%s", d.ReverseTable.SourceSchemaName, d.ReverseTable.SourceTableName), fmt.Sprintf("%s.%s", d.ReverseTable.TargetSchemaName, d.ReverseTable.TargetTableName), "Create Table"},
		})
	}

	sqlRev.WriteString(fmt.Sprintf("%v\n", sw.Render()))
	sqlRev.WriteString("*/\n")

	sqlRev.WriteString(d.CreateTable + "\n")
	if len(d.CreateIndex) > 0 {
		sqlRev.WriteString(strings.Join(d.CreateIndex, "\n") + "\n")
	}
	if d.CreateTableComment != "" {
		sqlRev.WriteString(d.CreateTableComment + "\n")

	}
	if len(d.CreateColumnComment) > 0 {
		sqlRev.WriteString(strings.Join(d.CreateColumnComment, "\n") + "\n")
	}

	// 文件写入
	if sqlRev.String() != "" {
		if _, err := fmt.Fprintln(d.RevFileMW, sqlRev.String()); err != nil {
			return err
		}
	}
	return nil
}

func (d *ReverseWriter) String() string {
	jsonStr, _ := json.Marshal(d)
	return string(jsonStr)
}

func CompatibilityDBTips(file *FileMW, sourceSchema string, errCompatibility map[string][]map[string]string, viewTables []string) error {
	startTime := time.Now()
	// 兼容提示
	if len(errCompatibility) > 0 {
		for tableName, info := range errCompatibility {
			var sqlComp strings.Builder

			sqlComp.WriteString("/*\n")
			sqlComp.WriteString(" mysql table maybe oracle has compatibility, please manual process\n")
			sqlComp.WriteString(" - mysql table character and collation current isn't support\n")
			sqlComp.WriteString(" - mysql table character and column character isn't the same, and column character currently isn't support\n")
			sqlComp.WriteString(" - mysql table collation and column collation isn't the same, and column collation currently isn't support\n")

			t := table.NewWriter()
			t.SetStyle(table.StyleLight)
			t.SetTitle(fmt.Sprintf("TABLE: %s.%s", sourceSchema, tableName))
			t.Style().Title.Align = text.Align(text.AlignCenter)

			t.AppendHeader(table.Row{"TABLE CHARACTER", "TABLE COLLATION", "COLUMN CHARACTER", "COLUMN COLLATION", "COLUMN TYPE", "SUGGEST"})
			for _, compINFO := range info {
				t.AppendRows([]table.Row{
					{compINFO["TableCharacterSet"], compINFO["TableCollation"], compINFO["ColumnCharacterSet"],
						compINFO["ColumnCollation"], compINFO["ColumnType"], "Manual Process Table"},
				})
			}
			sqlComp.WriteString(t.Render() + "\n")
			sqlComp.WriteString("*/\n")

			if _, err := fmt.Fprintln(file, sqlComp.String()); err != nil {
				return err
			}
		}
	}

	if len(viewTables) > 0 {
		var sqlComp strings.Builder

		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" mysql table maybe oracle has compatibility, please manual process\n")
		sqlComp.WriteString(" - mysql view current isn't support\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"SCHEMA", "TABLE NAME", "TABLE TYPE", "SUGGEST"})

		for _, viewName := range viewTables {
			t.AppendRows([]table.Row{{sourceSchema, viewName, "VIEW", "Manual Process Table"}})
		}
		sqlComp.WriteString(t.Render() + "\n")
		sqlComp.WriteString("*/\n")

		if _, err := fmt.Fprintln(file, sqlComp.String()); err != nil {
			return err
		}
	}

	endTime := time.Now()
	zap.L().Info("output oracle to oracle compatibility tips",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
