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
package check

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"github.com/tatsushid/go-prettytable"

	"github.com/wentaojin/transferdb/pkg/reverser"

	"go.uber.org/zap"
)

type DiffWriter struct {
	SourceSchemaName string
	TargetSchemaName string
	TableName        string
	Engine           *service.Engine
	*FileMW
}

type FileMW struct {
	Mutex  sync.Mutex
	Writer io.Writer
}

func NewDiffWriter(sourceSchemaName, targetSchemaName, tableName string, engine *service.Engine, fileMW *FileMW) *DiffWriter {
	return &DiffWriter{
		SourceSchemaName: sourceSchemaName,
		TargetSchemaName: targetSchemaName,
		TableName:        tableName,
		Engine:           engine,
		FileMW:           fileMW,
	}
}

func (d *FileMW) Write(b []byte) (n int, err error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	return d.Writer.Write(b)
}

// 表结构对比
// 以上游 oracle 表结构信息为基准，对比下游 MySQL 表结构
// 1、若上游存在，下游不存在，则输出记录，若上游不存在，下游存在，则默认不输出
// 2、忽略上下游不同索引名、约束名对比，只对比下游是否存在同等约束下同等字段是否存在
// 3、分区只对比分区类型、分区键、分区表达式等，不对比具体每个分区下的情况
func (d *DiffWriter) DiffOracleAndMySQLTable() error {
	startTime := time.Now()
	service.Logger.Info("check table start",
		zap.String("oracle table", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
		zap.String("mysql table", fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TableName)))

	service.Logger.Info("check table",
		zap.String("get oracle table", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
		zap.String("get mysql table", fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TableName)))
	oracleTable, err := NewOracleTableINFO(d.SourceSchemaName, d.TableName, d.Engine)
	if err != nil {
		return err
	}

	mysqlTable, mysqlVersion, err := NewMySQLTableINFO(d.TargetSchemaName, d.TableName, d.Engine)
	if err != nil {
		return err
	}

	isTiDB := false
	if strings.Contains(mysqlVersion, "TiDB") {
		isTiDB = true
	}

	// 输出格式：表结构一致不输出，只输出上下游不一致信息且输出以下游可执行 SQL 输出
	var builder strings.Builder

	// 表类型检查
	service.Logger.Info("check table",
		zap.String("table partition type check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if oracleTable.IsPartition != mysqlTable.IsPartition {
		builder.WriteString(fmt.Sprintf("-- oracle table [%s.%s]\n", d.SourceSchemaName, d.TableName))
		builder.WriteString(fmt.Sprintf("-- mysql table [%s.%s]\n", d.TargetSchemaName, d.TableName))
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table type is different from mysql table type\n"))
		builder.WriteString(fmt.Sprintf(" oracle table is partition type [%t]\n", oracleTable.IsPartition))
		builder.WriteString(fmt.Sprintf(" mysql table is partition type [%t]\n", mysqlTable.IsPartition))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("-- the above info comes from oracle table [%s.%s]\n", d.SourceSchemaName, d.TableName))
		builder.WriteString(fmt.Sprintf("-- the above info comes from mysql table [%s.%s]\n", d.TargetSchemaName, d.TableName))
		if _, err := fmt.Fprintln(d.FileMW, builder.String()); err != nil {
			return err
		}

		service.Logger.Warn("table type different",
			zap.String("oracle table", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
			zap.String("mysql table", fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TableName)),
			zap.String("msg", builder.String()))
		return nil
	}

	// 表注释检查
	service.Logger.Info("check table",
		zap.String("table comment check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if oracleTable.TableComment != mysqlTable.TableComment {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table comment [%s]\n", oracleTable.TableComment))
		builder.WriteString(fmt.Sprintf(" mysql table comment [%s]\n", mysqlTable.TableComment))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s COMMENT '%s';\n", d.TargetSchemaName, d.TableName, oracleTable.TableComment))
	}

	// 表字符集以及排序规则检查
	service.Logger.Info("check table",
		zap.String("table character set and collation check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if !strings.Contains(mysqlTable.TableCharacterSet, OracleUTF8CharacterSet) || !strings.Contains(mysqlTable.TableCollation, OracleCollationBin) {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table character set [%s], collation [%s]\n", oracleTable.TableCharacterSet, oracleTable.TableCollation))
		builder.WriteString(fmt.Sprintf(" mysql table character set [%s], collation [%s]\n", mysqlTable.TableCharacterSet, mysqlTable.TableCollation))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s CHARACTER SET = %s, COLLATE = %s;\n", d.TargetSchemaName, d.TableName, MySQLCharacterSet, MySQLCollation))
	}

	// 表约束、索引以及分区检查
	service.Logger.Info("check table",
		zap.String("table pk and uk constraint check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	diffPU, isOK := utils.IsEqualStruct(oracleTable.PUConstraints, mysqlTable.PUConstraints)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table primary key and unique key\n")
		builder.WriteString(" mysql table primary key and unique key\n")
		builder.WriteString("*/\n")
		for _, pu := range diffPU {
			value, ok := pu.(ConstraintPUKey)
			if ok {
				switch value.ConstraintType {
				case "PK":
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY(%s);\n", d.TargetSchemaName, d.TableName, value.ConstraintColumn))
					continue
				case "UK":
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD UNIQUE(%s);\n", d.TargetSchemaName, d.TableName, value.ConstraintColumn))
					continue
				default:
					return fmt.Errorf("table constraint primary and unique key diff failed: not support type [%s]", value.ConstraintType)
				}
			}
			return fmt.Errorf("table constraint primary and unique key assert ConstraintPUKey failed")
		}
	}

	// TiDB 版本排除外键以及检查约束
	if !isTiDB {
		service.Logger.Info("check table",
			zap.String("table fk constraint check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
		diffFK, isOK := utils.IsEqualStruct(oracleTable.ForeignConstraints, mysqlTable.ForeignConstraints)
		if !isOK {
			builder.WriteString("/*\n")
			builder.WriteString(" oracle table foreign key\n")
			builder.WriteString(" mysql table foreign key\n")
			builder.WriteString("*/\n")
			for _, fk := range diffFK {
				value, ok := fk.(ConstraintForeign)
				if ok {
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD FOREIGN KEY(%s) REFERENCES %s.%s(%s）ON DELETE %s;\n", d.TargetSchemaName, d.TableName, value.ColumnName, d.TargetSchemaName, value.ReferencedTableName, value.ReferencedColumnName, value.DeleteRule))
					continue
				}
				return fmt.Errorf("table constraint foreign key assert ConstraintForeign failed")
			}
		}

		var dbVersion string
		if strings.Contains(mysqlVersion, MySQLVersionDelimiter) {
			dbVersion = strings.Split(mysqlVersion, MySQLVersionDelimiter)[0]
		} else {
			dbVersion = mysqlVersion
		}
		if utils.VersionOrdinal(dbVersion) > utils.VersionOrdinal(MySQLCheckConsVersion) {
			service.Logger.Info("check table",
				zap.String("table ck constraint check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
			diffCK, isOK := utils.IsEqualStruct(oracleTable.CheckConstraints, mysqlTable.CheckConstraints)
			if !isOK {
				builder.WriteString("/*\n")
				builder.WriteString(" oracle table check key\n")
				builder.WriteString(" mysql table check key\n")
				builder.WriteString("*/\n")
				for _, ck := range diffCK {
					value, ok := ck.(ConstraintCheck)
					if ok {
						builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK(%s);\n",
							d.TargetSchemaName, d.TableName, fmt.Sprintf("%s_check_key", d.TableName), value.ConstraintExpression))
						continue
					}
					return fmt.Errorf("table constraint check key assert ConstraintCheck failed")
				}
			}
		}
	}

	service.Logger.Info("check table",
		zap.String("table indexes check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	diffIndex, isOK := utils.IsEqualStruct(oracleTable.Indexes, mysqlTable.Indexes)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table indexes\n")
		builder.WriteString(" mysql table indexes\n")
		builder.WriteString("*/\n")
		for _, idx := range diffIndex {
			value, ok := idx.(Index)
			if ok {
				if value.Uniqueness == "NONUNIQUE" {
					builder.WriteString(fmt.Sprintf("CREATE INDEX %s ON %s.%s(%s);\n",
						fmt.Sprintf("idx_%s", strings.ReplaceAll(value.IndexColumn, ",", "_")), d.TargetSchemaName, d.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "UNIQUE" {
					builder.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s(%s);\n",
						fmt.Sprintf("idx_%s_unique", strings.ReplaceAll(value.IndexColumn, ",", "_")), d.TargetSchemaName, d.TableName, value.IndexColumn))
					continue
				}
			}
			return fmt.Errorf("table index assert Index failed")
		}
	}

	service.Logger.Info("check table",
		zap.String("table partition check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if mysqlTable.IsPartition && oracleTable.IsPartition {
		diffParts, isOK := utils.IsEqualStruct(oracleTable.Partitions, mysqlTable.Partitions)
		if !isOK {
			builder.WriteString("/*\n")
			builder.WriteString(" oracle table partitions\n")
			builder.WriteString(" mysql table partitions\n")
			builder.WriteString("*/\n")
			builder.WriteString("-- oracle partition info exist, mysql partition isn't exist, please manual modify\n")
			for _, part := range diffParts {
				value, ok := part.(Partition)
				if ok {
					partJSON, err := json.Marshal(value)
					if err != nil {
						return err
					}
					builder.WriteString(fmt.Sprintf("# oracle partition info: %s, ", partJSON))
					continue
				}
				return fmt.Errorf("table paritions assert Partition failed")
			}
		}
	}

	// 表字段检查
	// 注释格式化
	textTable, err := prettytable.NewTable([]prettytable.Column{
		{Header: "column"},
		{Header: "oracle"},
		{Header: "mysql"},
		{Header: "suggest"},
	}...)
	if err != nil {
		return err
	}
	textTable.Separator = " | "

	var (
		diffColumnMsgs    []string
		createColumnMetas []string
	)

	for oracleColName, oracleColInfo := range oracleTable.Columns {
		mysqlColInfo, ok := mysqlTable.Columns[oracleColName]
		if ok {
			diffColumnMsg, err := CheckOracleTableMapRule(
				d.SourceSchemaName,
				d.TargetSchemaName,
				d.TableName,
				oracleColName,
				oracleColInfo,
				mysqlColInfo,
				textTable,
			)
			if err != nil {
				return err
			}
			if diffColumnMsg != "" {
				diffColumnMsgs = append(diffColumnMsgs, diffColumnMsg)
			}
			continue
		}
		columnMeta, err := reverser.ReverseOracleTableColumnMapRule(
			d.SourceSchemaName,
			d.TableName,
			oracleColName,
			oracleColInfo.DataType,
			oracleColInfo.NULLABLE,
			oracleColInfo.Comment,
			oracleColInfo.DataDefault,
			oracleColInfo.DataScale,
			oracleColInfo.DataPrecision,
			oracleColInfo.DataLength,
			[]reverser.ColumnType{})
		if err != nil {
			return err
		}
		if columnMeta != "" {
			createColumnMetas = append(createColumnMetas, columnMeta)
		}
	}

	if textTable.String() != "" && len(diffColumnMsgs) != 0 {
		service.Logger.Info("check table",
			zap.String("table column info check, generate fixed sql", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))

		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table columns info is different from mysql\n"))
		builder.WriteString(fmt.Sprintf(" %s\n", textTable.String()))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("-- oracle table columns info is different from mysql, generate fixed sql\n"))
		for _, diffColMsg := range diffColumnMsgs {
			builder.WriteString(diffColMsg)
		}
	}

	if len(createColumnMetas) != 0 {
		service.Logger.Info("check table",
			zap.String("table column info check, generate created sql", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table columns info isn't exist in mysql, generate created sql\n"))
		builder.WriteString("*/\n")
		for _, columnMeta := range createColumnMetas {
			builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s;\n",
				d.TargetSchemaName, d.TableName, columnMeta))
		}
	}

	// diff 记录不为空
	if builder.String() != "" {
		builder.WriteString(fmt.Sprintf("-- the above info comes from oracle table [%s.%s]\n", d.SourceSchemaName, d.TableName))
		builder.WriteString(fmt.Sprintf("-- the above info comes from mysql table [%s.%s]\n", d.TargetSchemaName, d.TableName))
		if _, err := fmt.Fprintln(d.FileMW, builder.String()); err != nil {
			return err
		}
	}

	endTime := time.Now()
	service.Logger.Info("check table finished",
		zap.String("oracle table", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
		zap.String("mysql table", fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TableName)),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func CheckOracleTableMapRule(
	sourceSchema, targetSchema, tableName, columnName string,
	oracleColInfo, mysqlColInfo Column, textTable *prettytable.Table) (string, error) {
	// 字段精度类型转换
	oracleDataLength, err := strconv.Atoi(oracleColInfo.DataLength)
	if err != nil {
		return "", fmt.Errorf("oracle schema [%s] table [%s] column data_length string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataPrecision, err := strconv.Atoi(oracleColInfo.DataPrecision)
	if err != nil {
		return "", fmt.Errorf("oracle schema [%s] table [%s] column data_precision string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataScale, err := strconv.Atoi(oracleColInfo.DataScale)
	if err != nil {
		return "", fmt.Errorf("oracle schema [%s] table [%s] column data_scale string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleColumnCharLength, err := strconv.Atoi(oracleColInfo.CharLength)
	if err != nil {
		return "", fmt.Errorf("oracle schema [%s] table [%s] column char_length string to int failed: %v", sourceSchema, tableName, err)
	}

	mysqlDataLength, err := strconv.Atoi(mysqlColInfo.DataLength)
	if err != nil {
		return "", fmt.Errorf("mysql schema table [%s.%s] column data_length string to int failed: %v",
			targetSchema, tableName, err)
	}
	mysqlDataPrecision, err := strconv.Atoi(mysqlColInfo.DataPrecision)
	if err != nil {
		return "", fmt.Errorf("mysql schema table [%s.%s] reverser column data_precision string to int failed: %v", targetSchema, tableName, err)
	}
	mysqlDataScale, err := strconv.Atoi(mysqlColInfo.DataScale)
	if err != nil {
		return "", fmt.Errorf("mysql schema table [%s.%s] reverser column data_scale string to int failed: %v", targetSchema, tableName, err)
	}

	// 字段默认值、注释判断
	mysqlDataType := strings.ToUpper(mysqlColInfo.DataType)
	oracleDataType := strings.ToUpper(oracleColInfo.DataType)
	var (
		oracleDataDefault    string
		oracleColumnComment  string
		fixedMsg             string
		oracleColumnCharUsed string
	)
	if oracleColInfo.DataDefault == "" {
		oracleDataDefault = "NULL"
	} else {
		oracleDataDefault = oracleColInfo.DataDefault
	}
	if oracleColInfo.Comment == "" {
		oracleColumnComment = ""
	} else {
		oracleColumnComment = oracleColInfo.Comment
	}

	if oracleColInfo.CharUsed == "C" {
		oracleColumnCharUsed = "char"
	} else {
		oracleColumnCharUsed = "bytes"
	}
	// 字段类型判断
	// CHARACTER SET %s COLLATE %s（OnLy 作用字符类型）
	switch oracleDataType {
	// 数字
	case "NUMBER":
		switch {
		case oracleDataScale > 0:
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("NUMBER(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("DECIMAL(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
		case oracleDataScale == 0:
			switch {
			case oracleDataPrecision == 0 && oracleDataScale == 0:
				//MySQL column type  NUMERIC would convert to DECIMAL(11,0)
				//buildInColumnType = "NUMERIC"
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
						mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("DECIMAL(11,0) %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					"DECIMAL(11,0)",
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
			case oracleDataPrecision >= 1 && oracleDataPrecision < 3:
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "TINYINT" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("TINYINT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}
				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					"TINYINT",
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
			case oracleDataPrecision >= 3 && oracleDataPrecision < 5:
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "SMALLINT" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("SMALLINT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}
				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					"SMALLINT",
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
			case oracleDataPrecision >= 5 && oracleDataPrecision < 9:
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "INT" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("INT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}
				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					"INT",
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
			case oracleDataPrecision >= 9 && oracleDataPrecision < 19:
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "BIGINT" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("BIGINT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}
				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					"BIGINT",
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
			case oracleDataPrecision >= 19 && oracleDataPrecision <= 38:
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("DECIMAL(%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}
				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					fmt.Sprintf("DECIMAL(%d)", oracleDataPrecision),
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
			default:
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("DECIMAL(%d,4) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}
				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					fmt.Sprintf("DECIMAL(%d,4)", oracleDataPrecision),
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
			}
		}
		return fixedMsg, nil
	case "DECIMAL":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		switch {
		case oracleDataScale == 0 && oracleDataPrecision == 0:
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("DECIMAL %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
					mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("DECIMAL %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				"DECIMAL",
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		default:
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
					mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("DECIMAL(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
	case "DEC":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		switch {
		case oracleDataScale == 0 && oracleDataPrecision == 0:
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("DECIMAL %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
					mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("DECIMAL %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				"DECIMAL",
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		default:
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
					mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("DECIMAL(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
	case "DOUBLE PRECISION":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DOUBLE PRECISION" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("DOUBLE PRECISION %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("DOUBLE PRECISION %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE PRECISION",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "FLOAT":
		if oracleDataPrecision == 0 {
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "FLOAT" && len(diffCols) == 0 && isOK {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("FLOAT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
					mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("FLOAT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				"FLOAT",
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DOUBLE" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("FLOAT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("DOUBLE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "INTEGER":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "INT" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("INTEGER %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("INT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"INT",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "INT":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "INT" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("INTEGER %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("INT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"INT",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "REAL":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DOUBLE" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("REAL %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("DOUBLE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "NUMERIC":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "NUMERIC" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("NUMERIC(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("NUMERIC(%d,%d) %s DEFAULT %s COMMENT '%s'", oracleDataPrecision, oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NUMERIC(%d,%d)", oracleDataPrecision, oracleDataScale),
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "BINARY_FLOAT":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DOUBLE" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("BINARY_FLOAT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("DOUBLE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "BINARY_DOUBLE":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DOUBLE" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("BINARY_DOUBLE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("DOUBLE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "SMALLINT":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("SMALLINT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("DECIMAL(38) %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"DECIMAL(38)",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil

	// 字符
	case "BFILE":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "VARCHAR" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("BFILE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("VARCHAR(255) %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"VARCHAR(255)",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "CHARACTER":
		if oracleDataLength < 256 {
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "CHARACTER" && len(diffCols) == 0 && isOK {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("CHARACTER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType,
					mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("CHARACTER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("CHARACTER(%d)", oracleDataLength),
				MySQLCharacterSet,
				MySQLCollation,
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "VARCHAR" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("CHARACTER(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("VARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "LONG":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "LONGTEXT" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("LONG %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("LONGTEXT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "LONG RAW":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "LONGBLOB" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("LONG RAW %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("LONGBLOB %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"LONGBLOB",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "NCHAR VARYING":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "NCHAR VARYING" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("NCHAR VARYING %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("NCHAR VARYING(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NCHAR VARYING(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "NCLOB":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "TEXT" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("NCLOB %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("TEXT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"TEXT",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "RAW":
		if oracleDataLength < 256 {
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "BINARY" && len(diffCols) == 0 && isOK {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("RAW(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("BINARY(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("BINARY(%d)", oracleDataLength),
				MySQLCharacterSet,
				MySQLCollation,
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "VARBINARY" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("RAW(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("VARBINARY(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARBINARY(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "ROWID":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "CHAR" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("ROWID %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("CHAR(10) %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"CHAR(10)",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "UROWID":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "VARCHAR" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("UROWID %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("VARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "VARCHAR":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "VARCHAR" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("VARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("VARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "XMLTYPE":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "LONGTEXT" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("XMLTYPE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("LONGTEXT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil

	// 二进制
	case "CLOB":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "LONGTEXT" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("CLOB %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("LONGTEXT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "BLOB":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "BLOB" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("BLOB %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("BLOB %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"BLOB",
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil

	// 时间
	case "DATE":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "DATETIME" && len(diffCols) == 0 && isOK {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("DATE %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
				mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("DATETIME %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			"DATETIME",
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil

	// oracle 字符类型 bytes/char 判断 B/C
	// CHAR、NCHAR、VARCHAR2、NVARCHAR2( oracle 字符类型 B/C)
	case "CHAR":
		if oracleDataLength < 256 {
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "CHAR" && len(diffCols) == 0 && isOK && oracleColumnCharUsed == "char" && oracleColumnCharLength == mysqlDataLength {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("CHAR(%d %s) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColumnCharUsed, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("CHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("CHAR(%d)", oracleDataLength),
				MySQLCharacterSet,
				MySQLCollation,
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "VARCHAR" && len(diffCols) == 0 && isOK && oracleColumnCharUsed == "char" && oracleColumnCharLength == mysqlDataLength {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("CHAR(%d %s) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColumnCharUsed, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("VARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "NCHAR":
		if oracleDataLength < 256 {
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "NCHAR" && len(diffCols) == 0 && isOK && oracleColumnCharUsed == "char" && oracleColumnCharLength == mysqlDataLength {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("NCHAR(%d %s) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColumnCharUsed, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("NCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("NCHAR(%d)", oracleDataLength),
				MySQLCharacterSet,
				MySQLCollation,
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "NVARCHAR" && len(diffCols) == 0 && isOK && oracleColumnCharUsed == "char" && oracleColumnCharLength == mysqlDataLength {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("NVARCHAR(%d %s) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColumnCharUsed, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("NVARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NVARCHAR(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "VARCHAR2":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "VARCHAR" && len(diffCols) == 0 && isOK && oracleColumnCharUsed == "char" && oracleColumnCharLength == mysqlDataLength {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("VARCHAR2(%d %s) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColumnCharUsed, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("VARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil
	case "NVARCHAR2":
		diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
		if mysqlDataType == "NVARCHAR" && len(diffCols) == 0 && isOK && oracleColumnCharUsed == "char" && oracleColumnCharLength == mysqlDataLength {
			return "", nil
		}
		if err := textTable.AddRow(
			columnName,
			fmt.Sprintf("NVARCHAR2(%d %s) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColumnCharUsed, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
			fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
			fmt.Sprintf("NVARCHAR(%d) %s DEFAULT %s COMMENT '%s'", oracleDataLength, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
			return "", err
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NVARCHAR(%d)", oracleDataLength),
			MySQLCharacterSet,
			MySQLCollation,
			oracleColInfo.NULLABLE,
			oracleDataDefault,
			oracleColumnComment,
		)
		return fixedMsg, nil

	// 默认其他类型
	default:
		if strings.Contains(oracleDataType, "INTERVAL") {
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "VARCHAR" && len(diffCols) == 0 && isOK {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("%s %s DEFAULT %s COMMENT '%s'", oracleDataType, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("VARCHAR(30) %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				"VARCHAR(30)",
				MySQLCharacterSet,
				MySQLCollation,
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		} else if strings.Contains(oracleDataType, "TIMESTAMP") {
			if oracleDataScale == 0 {
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "DATETIME" && len(diffCols) == 0 && isOK {
					return "", nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("%s %s DEFAULT %s COMMENT '%s'", oracleDataType, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
					fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
						mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
					fmt.Sprintf("DATETIME %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
					return "", err
				}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
					targetSchema,
					tableName,
					columnName,
					"DATETIME",
					oracleColInfo.NULLABLE,
					oracleDataDefault,
					oracleColumnComment,
				)
				return fixedMsg, nil
			}
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "DATETIME" && len(diffCols) == 0 && isOK {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("%s %s DEFAULT %s COMMENT '%s'", oracleDataType, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d,%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataPrecision,
					mysqlDataScale, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("DATETIME(%d) %s DEFAULT %s COMMENT '%s'", oracleDataScale, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DATETIME(%d)", oracleDataScale),
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		} else {
			diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
			if mysqlDataType == "TEXT" && len(diffCols) == 0 && isOK {
				return "", nil
			}
			if err := textTable.AddRow(
				columnName,
				fmt.Sprintf("%s %s DEFAULT %s COMMENT '%s'", oracleDataType, oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment),
				fmt.Sprintf("%s(%d) %s DEFAULT %s COMMENT '%s'", mysqlDataType, mysqlDataLength, mysqlColInfo.NULLABLE, mysqlColInfo.DataDefault, mysqlColInfo.Comment),
				fmt.Sprintf("TEXT %s DEFAULT %s COMMENT '%s'", oracleColInfo.NULLABLE, oracleDataDefault, oracleColumnComment)); err != nil {
				return "", err
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT '%s';\n",
				targetSchema,
				tableName,
				columnName,
				"TEXT",
				MySQLCharacterSet,
				MySQLCollation,
				oracleColInfo.NULLABLE,
				oracleDataDefault,
				oracleColumnComment,
			)
			return fixedMsg, nil
		}
	}
}
