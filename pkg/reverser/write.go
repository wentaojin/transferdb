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
package reverser

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/service"

	"github.com/wentaojin/transferdb/utils"

	"github.com/jedib0t/go-pretty/v6/table"
)

type ReverseWriter struct {
	DBType          string
	DBVersion       string
	CreateTable     string
	CreateFK        []string
	CreateCK        []string
	CreateCompIndex []string
	ReverseTable    Table
	RevFileMW       *FileMW
	CompFileMW      *FileMW
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
	mysqlVersion, err := t.Engine.GetMySQLDBVersion()
	if err != nil {
		return nil, err
	}

	var dbVersion string

	if strings.ToUpper(t.TargetDBType) == utils.TiDBTargetDBType {
		dbVersion = mysqlVersion
	} else {
		if strings.Contains(mysqlVersion, utils.MySQLVersionDelimiter) {
			dbVersion = strings.Split(mysqlVersion, utils.MySQLVersionDelimiter)[0]
		} else {
			dbVersion = mysqlVersion
		}
	}

	// 表名转换
	modifyTableName := changeOracleTableName(t.SourceTableName, t.TargetTableName)
	t.TargetTableName = modifyTableName

	tableStruct, compIndexINFO, err := t.GenCreateTableSQL(modifyTableName)
	if err != nil {
		return nil, err
	}

	// 兼容项
	fkSQL, err := t.GenCreateFKSQL(modifyTableName)
	if err != nil {
		return nil, err
	}
	ckSQL, err := t.GenCreateCKSQL(modifyTableName)
	if err != nil {
		return nil, err
	}

	return &ReverseWriter{
		DBType:          t.TargetDBType,
		DBVersion:       dbVersion,
		CreateTable:     tableStruct,
		CreateFK:        fkSQL,
		CreateCK:        ckSQL,
		CreateCompIndex: compIndexINFO,
		ReverseTable:    t,
		RevFileMW:       revFileMW,
		CompFileMW:      compFileMW,
	}, nil

}

func (d *ReverseWriter) Reverse() error {
	var (
		sqlRev  strings.Builder
		sqlComp strings.Builder
	)

	// 表 with 主键
	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(fmt.Sprintf(" oracle table reverse sql \n"))

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"#", "ORACLE TABLE TYPE", "ORACLE", "MYSQL", "SUGGEST"})
	sw.AppendRows([]table.Row{
		{"TABLE", d.ReverseTable.SourceTableType, fmt.Sprintf("%s.%s", d.ReverseTable.SourceSchemaName, d.ReverseTable.SourceTableName), fmt.Sprintf("%s.%s", d.ReverseTable.TargetSchemaName, d.ReverseTable.TargetTableName), "Create Table"},
	})
	sqlRev.WriteString(fmt.Sprintf("%v\n", sw.Render()))
	sqlRev.WriteString("*/\n")
	sqlRev.WriteString(d.CreateTable + "\n\n")

	// 兼容项处理
	if len(d.CreateFK) > 0 || len(d.CreateCK) > 0 || len(d.CreateCompIndex) > 0 {
		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(fmt.Sprintf(" oracle table index or consrtaint maybe mysql has compatibility, skip\n"))
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
		tw.AppendRows([]table.Row{
			{"TABLE", fmt.Sprintf("%s.%s", d.ReverseTable.SourceSchemaName, d.ReverseTable.SourceTableName), fmt.Sprintf("%s.%s", d.ReverseTable.TargetSchemaName, d.ReverseTable.TargetTableName), "Create Index Or Constraints"}})

		sqlComp.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		sqlComp.WriteString("*/\n")
	}

	// 外键约束、检查约束
	if d.DBType != utils.TiDBTargetDBType {
		if len(d.CreateFK) > 0 {
			for _, sql := range d.CreateFK {
				sqlRev.WriteString(sql + "\n")
			}
		}

		if utils.VersionOrdinal(d.DBVersion) > utils.VersionOrdinal(utils.MySQLCheckConsVersion) {
			if len(d.CreateCK) > 0 {
				for _, sql := range d.CreateCK {
					sqlRev.WriteString(sql + "\n")
				}
			}
		} else {
			// 增加不兼容性语句
			if len(d.CreateCK) > 0 {
				for _, sql := range d.CreateCK {
					sqlComp.WriteString(sql + "\n")
				}
			}
		}
		// 增加不兼容性语句
		if len(d.CreateCompIndex) > 0 {
			for _, sql := range d.CreateCompIndex {
				sqlComp.WriteString(sql + "\n")
			}
		}
		// 文件写入
		if sqlRev.String() != "" {
			if _, err := fmt.Fprintln(d.RevFileMW, sqlRev.String()); err != nil {
				return err
			}
		}
		if sqlComp.String() != "" {
			if _, err := fmt.Fprintln(d.CompFileMW, sqlComp.String()); err != nil {
				return err
			}
		}
		return nil
	}

	// TiDB 增加不兼容性语句
	if len(d.CreateFK) > 0 {
		for _, sql := range d.CreateFK {
			sqlComp.WriteString(sql + "\n")
		}
	}
	if len(d.CreateCK) > 0 {
		for _, sql := range d.CreateCK {
			sqlComp.WriteString(sql + "\n")
		}
	}
	if len(d.CreateCompIndex) > 0 {
		for _, sql := range d.CreateCompIndex {
			sqlComp.WriteString(sql + "\n")
		}
	}

	// 文件写入
	if sqlRev.String() != "" {
		if _, err := fmt.Fprintln(d.RevFileMW, sqlRev.String()); err != nil {
			return err
		}
	}
	if sqlComp.String() != "" {
		if _, err := fmt.Fprintln(d.CompFileMW, sqlComp.String()); err != nil {
			return err
		}
	}
	return nil
}

func (d *ReverseWriter) String() string {
	jsonStr, _ := json.Marshal(d)
	return string(jsonStr)
}

func GenCreateSchema(file *FileMW, engine *service.Engine, sourceSchema, targetSchema, nlsComp string) error {
	startTime := time.Now()
	var (
		sqlRev          strings.Builder
		schemaCollation string
	)

	oraDBVersion, err := engine.GetOracleDBVersion()
	if err != nil {
		return err
	}

	oraCollation := false
	if utils.VersionOrdinal(oraDBVersion) >= utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
		oraCollation = true
	}
	if oraCollation {
		schemaCollation, err = engine.GetOracleSchemaCollation(sourceSchema)
		if err != nil {
			return err
		}
	}

	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(fmt.Sprintf(" oracle schema reverse mysql database\n"))
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
	t.AppendRows([]table.Row{
		{"Schema", sourceSchema, targetSchema, "Create Schema"},
	})
	sqlRev.WriteString(t.Render() + "\n")
	sqlRev.WriteString("*/\n")

	if oraCollation {
		if _, ok := utils.OracleCollationMap[strings.ToUpper(schemaCollation)]; !ok {
			return fmt.Errorf("oracle schema collation [%s] isn't support", schemaCollation)
		}
		sqlRev.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET %s COLLATE %s;\n\n", strings.ToUpper(targetSchema), strings.ToLower(utils.MySQLCharacterSet), utils.OracleCollationMap[strings.ToUpper(schemaCollation)]))
	} else {
		if _, ok := utils.OracleCollationMap[strings.ToUpper(nlsComp)]; !ok {
			return fmt.Errorf("oracle db nls_comp collation [%s] isn't support", nlsComp)
		}
		sqlRev.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET %s COLLATE %s;\n\n", strings.ToUpper(targetSchema), strings.ToLower(utils.MySQLCharacterSet), utils.OracleCollationMap[strings.ToUpper(nlsComp)]))
	}
	if _, err = fmt.Fprintln(file, sqlRev.String()); err != nil {
		return err
	}
	endTime := time.Now()
	zap.L().Info("output oracle to mysql schema create sql",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func CompatibilityDBTips(file *FileMW, sourceSchema string, partition, temporary, clustered []string) error {
	startTime := time.Now()
	// 兼容提示
	if len(partition) > 0 || len(temporary) > 0 || len(clustered) > 0 {
		var sqlComp strings.Builder

		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(fmt.Sprintf(" oracle table maybe mysql has compatibility, will convert to normal table, please manual process\n"))
		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"SCHEMA", "TABLE NAME", "ORACLE TABLE TYPE", "SUGGEST"})

		if len(partition) > 0 {
			for _, part := range partition {
				t.AppendRows([]table.Row{
					{sourceSchema, part, "Partition", "Manual Process Table"},
				})
			}
		}
		if len(temporary) > 0 {
			for _, temp := range temporary {
				t.AppendRows([]table.Row{
					{sourceSchema, temp, "Temporary", "Manual Process Table"},
				})
			}
		}
		if len(clustered) > 0 {
			for _, cd := range clustered {
				t.AppendRows([]table.Row{
					{sourceSchema, cd, "Clustered", "Manual Process Table"},
				})
			}
		}
		sqlComp.WriteString(t.Render() + "\n")
		sqlComp.WriteString("*/\n")
		if _, err := fmt.Fprintln(file, sqlComp.String()); err != nil {
			return err
		}
	}
	endTime := time.Now()
	zap.L().Info("output oracle to mysql compatibility tips",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
