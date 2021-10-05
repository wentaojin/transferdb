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
	oracleTable, err := NewOracleTableINFO(d.SourceSchemaName, d.TableName, d.Engine)
	if err != nil {
		return err
	}

	mysqlTable, err := NewMySQLTableINFO(d.TargetSchemaName, d.TableName, d.Engine)
	if err != nil {
		return err
	}

	// 输出格式：表结构一致不输出，只输出上下游不一致信息且输出以下游可执行 SQL 输出
	var builder strings.Builder

	// 表类型检查
	if oracleTable.IsPartition != mysqlTable.IsPartition {
		builder.WriteString(fmt.Sprintf("-- oracle table [%s.%s]", d.SourceSchemaName, d.TableName))
		builder.WriteString(fmt.Sprintf("-- mysql table [%s.%s]", d.TargetSchemaName, d.TableName))
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table type is different from mysql table type\n"))
		builder.WriteString(fmt.Sprintf(" oracle table is partition type [%t]\n", oracleTable.IsPartition))
		builder.WriteString(fmt.Sprintf(" mysql table is partition type [%t]\n", mysqlTable.IsPartition))
		builder.WriteString("*/\n")
		service.Logger.Warn("table type different",
			zap.String("oracle table", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
			zap.String("mysql table", fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TableName)),
			zap.String("msg", builder.String()))
		return nil
	}

	// 表注释检查
	if oracleTable.TableComment != mysqlTable.TableComment {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table comment [%s]\n", oracleTable.TableComment))
		builder.WriteString(fmt.Sprintf(" mysql table comment [%s]\n", mysqlTable.TableComment))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s COMMENT '%s';\n", d.TargetSchemaName, d.TableName, oracleTable.TableComment))
	}

	// 表约束、索引以及分区检查
	diffPU, isOK := utils.IsEqualStruct(oracleTable.PUConstraints, mysqlTable.PUConstraints)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table primary key and unique key")
		builder.WriteString(" mysql table primary key and unique key")
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

	diffFK, isOK := utils.IsEqualStruct(oracleTable.ForeignConstraints, mysqlTable.ForeignConstraints)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table foreign key")
		builder.WriteString(" mysql table foreign key")
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

	diffCK, isOK := utils.IsEqualStruct(oracleTable.CheckConstraints, mysqlTable.CheckConstraints)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table check key")
		builder.WriteString(" mysql table check key")
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

	diffIndex, isOK := utils.IsEqualStruct(oracleTable.Indexes, mysqlTable.Indexes)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table indexes")
		builder.WriteString(" mysql table indexes")
		builder.WriteString("*/\n")
		for _, idx := range diffIndex {
			value, ok := idx.(Index)
			if ok {
				if value.Uniqueness == "NONUNIQUE" {
					builder.WriteString(fmt.Sprintf("CREATE INDEX %s ON %s.%s(%s);\n",
						fmt.Sprintf("idx_%s", value.IndexColumn), d.TargetSchemaName, d.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "UNIQUE" {
					builder.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s(%s);\n",
						fmt.Sprintf("idx_%s_unique", value.IndexColumn), d.TargetSchemaName, d.TableName, value.IndexColumn))
					continue
				}
			}
			return fmt.Errorf("table index assert Index failed")
		}
	}

	diffParts, isOK := utils.IsEqualStruct(oracleTable.Partitions, mysqlTable.Partitions)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table partitions")
		builder.WriteString(" mysql table partitions")
		builder.WriteString("*/\n")
		builder.WriteString("# oracle partition info exist, mysql partition isn't exist, please manual modify")
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

	// 表字段检查
	// 注释格式化
	tbl, err := prettytable.NewTable([]prettytable.Column{
		{Header: "column"},
		{Header: "oracle"},
		{Header: "mysql"},
		{Header: "suggest"},
	}...)
	if err != nil {
		return err
	}
	tbl.Separator = " | "

	var fixedMsg []string

	for oracleColName, oracleColInfo := range oracleTable.Columns {
		mysqlColInfo, ok := mysqlTable.Columns[oracleColName]
		if ok {

			continue
		}
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table columns [%s] isn't exist in mysql", oracleColName))
		builder.WriteString("*/\n")
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
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s;\n",
			d.TargetSchemaName, d.TableName, columnMeta))
	}

	// diff 记录不为空
	if builder.String() != "" {
		if _, err := fmt.Fprintln(d.FileMW, builder.String()); err != nil {
			return err
		}
	}

	return nil
}

func CheckOracleTableMapRule(
	sourceSchema, targetSchema, tableName, columnName string,
	oracleColInfo, mysqlColInfo Column, textTable *prettytable.Table, fixedMsg []string) (string, []string, error) {
	oracleDataLength, err := strconv.Atoi(oracleColInfo.DataLength)
	if err != nil {
		return "", fixedMsg, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_length string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataPrecision, err := strconv.Atoi(oracleColInfo.DataPrecision)
	if err != nil {
		return "", fixedMsg, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_precision string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataScale, err := strconv.Atoi(oracleColInfo.DataScale)
	if err != nil {
		return "", fixedMsg, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_scale string to int failed: %v", sourceSchema, tableName, err)
	}

	mysqlDataLength, err := strconv.Atoi(mysqlColInfo.DataLength)
	if err != nil {
		return "", fixedMsg, fmt.Errorf("mysql schema table [%s.%s] column data_length string to int failed: %v",
			targetSchema, tableName, err)
	}
	mysqlDataPrecision, err := strconv.Atoi(mysqlColInfo.DataPrecision)
	if err != nil {
		return "", fixedMsg, fmt.Errorf("mysql schema table [%s.%s] reverser column data_precision string to int failed: %v", targetSchema, tableName, err)
	}
	mysqlDataScale, err := strconv.Atoi(mysqlColInfo.DataScale)
	if err != nil {
		return "", fixedMsg, fmt.Errorf("mysql schema table [%s.%s] reverser column data_scale string to int failed: %v", targetSchema, tableName, err)
	}

	mysqlDataType := strings.ToUpper(mysqlColInfo.DataType)
	if oracleColInfo.CharUsed == "UNKNOWN" {
		switch strings.ToUpper(oracleColInfo.DataType) {
		case "NUMBER":
			switch {
			case oracleDataScale > 0:
				diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
				if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
					return "", fixedMsg, nil
				}
				if err := textTable.AddRow(
					columnName,
					fmt.Sprintf("NUMBER(%d,%d)", oracleDataPrecision, oracleDataScale),
					fmt.Sprintf("%s(%d,%d)", mysqlDataType, mysqlDataPrecision, mysqlDataScale),
					fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale)); err != nil {
					return "", fixedMsg, err
				}
				if !strings.Contains(mysqlColInfo.Collation, OracleCollationBin) && !strings.Contains(mysqlColInfo.CharacterSet, OracleUTF8CharacterSet) {
					fixedMsg = append(fixedMsg, fmt.Sprintf("ALTER  TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT '%s';\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
						MySQLCharacterSet,
						MySQLCollation,
						oracleColInfo.NULLABLE,
						oracleColInfo.Comment,
					))
				}

			case oracleDataScale == 0:
				switch {
				case oracleDataPrecision == 0 && oracleDataScale == 0:
					//MySQL column type  NUMERIC would convert to DECIMAL(11,0)
					//buildInColumnType = "NUMERIC"
					diffCols, isOK := utils.IsEqualStruct(oracleColInfo.ColumnInfo, mysqlColInfo.ColumnInfo)
					if mysqlDataType == "DECIMAL" && len(diffCols) == 0 && isOK {
						return "", fixedMsg, nil
					}
					if err := textTable.AddRow(
						columnName,
						"NUMBER",
						fmt.Sprintf("%s(%d,%d)", mysqlDataType, mysqlDataPrecision, mysqlDataScale)); err != nil {
						return "", fixedMsg, err
					}
					fixedMsg = append(fixedMsg, fmt.Sprintf())
				case oracleDataPrecision >= 1 && oracleDataPrecision < 3:
					if mysqlDataType == "TINYINT" && oracleDataPrecision == mysqlDataPrecision && oracleDataScale == mysqlDataScale {
						return "", fixedMsg, nil
					}
					if err := textTable.AddRow(
						columnName,
						fmt.Sprintf("NUMBER(%d)", oracleDataPrecision),
						fmt.Sprintf("%s(%d,%d)", mysqlDataType, mysqlDataPrecision, mysqlDataScale)); err != nil {
						return "", fixedMsg, err
					}
				case oracleDataPrecision >= 3 && oracleDataPrecision < 5:
					if mysqlDataType == "SMALLINT" && oracleDataPrecision == mysqlDataPrecision && oracleDataScale == mysqlDataScale {
						return "", fixedMsg, nil
					}
					if err := textTable.AddRow(
						columnName,
						fmt.Sprintf("NUMBER(%d)", oracleDataPrecision),
						fmt.Sprintf("%s(%d,%d)", mysqlDataType, mysqlDataPrecision, mysqlDataScale)); err != nil {
						return "", fixedMsg, err
					}
				case oracleDataPrecision >= 5 && oracleDataPrecision < 9:
					if mysqlDataType == "INT" && oracleDataPrecision == mysqlDataPrecision && oracleDataScale == mysqlDataScale {
						return "", fixedMsg, nil
					}
					if err := textTable.AddRow(
						columnName,
						fmt.Sprintf("NUMBER(%d)", oracleDataPrecision),
						fmt.Sprintf("%s(%d,%d)", mysqlDataType, mysqlDataPrecision, mysqlDataScale)); err != nil {
						return "", fixedMsg, err
					}
				case oracleDataPrecision >= 9 && oracleDataPrecision < 19:
					if mysqlDataType == "BIGINT" && oracleDataPrecision == mysqlDataPrecision && oracleDataScale == mysqlDataScale {
						return "", fixedMsg, nil
					}
					if err := textTable.AddRow(
						columnName,
						fmt.Sprintf("NUMBER(%d)", oracleDataPrecision),
						fmt.Sprintf("%s(%d,%d)", mysqlDataType, mysqlDataPrecision, mysqlDataScale)); err != nil {
						return "", fixedMsg, err
					}
				case oracleDataPrecision >= 19 && oracleDataPrecision <= 38:
					if mysqlDataType == "DECIMAL" && oracleDataPrecision == mysqlDataPrecision && oracleDataScale == mysqlDataScale {
						return "", fixedMsg, nil
					}
					if err := textTable.AddRow(
						columnName,
						fmt.Sprintf("NUMBER(%d)", oracleDataPrecision),
						fmt.Sprintf("%s(%d,%d)", mysqlDataType, mysqlDataPrecision, mysqlDataScale)); err != nil {
						return "", fixedMsg, err
					}
				default:
					originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
					buildInColumnType = fmt.Sprintf("DECIMAL(%d,4)", dataPrecision)
				}
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "BFILE":
			originColumnType = "BFILE"
			buildInColumnType = "VARCHAR(255)"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "CHARACTER":
			originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
			if dataLength < 256 {
				buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
			}
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "CLOB":
			originColumnType = "CLOB"
			buildInColumnType = "LONGTEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "BLOB":
			originColumnType = "BLOB"
			buildInColumnType = "BLOB"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "DATE":
			originColumnType = "DATE"
			buildInColumnType = "DATETIME"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "DECIMAL":
			switch {
			case dataScale == 0 && dataPrecision == 0:
				originColumnType = "DECIMAL"
				buildInColumnType = "DECIMAL"
			default:
				originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "DEC":
			switch {
			case dataScale == 0 && dataPrecision == 0:
				originColumnType = "DECIMAL"
				buildInColumnType = "DECIMAL"
			default:
				originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "DOUBLE PRECISION":
			originColumnType = "DOUBLE PRECISION"
			buildInColumnType = "DOUBLE PRECISION"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "FLOAT":
			originColumnType = "FLOAT"
			if dataPrecision == 0 {
				buildInColumnType = "FLOAT"
			}
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "INTEGER":
			originColumnType = "INTEGER"
			buildInColumnType = "INT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "INT":
			originColumnType = "INTEGER"
			buildInColumnType = "INT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "LONG":
			originColumnType = "LONG"
			buildInColumnType = "LONGTEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "LONG RAW":
			originColumnType = "LONG RAW"
			buildInColumnType = "LONGBLOB"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "BINARY_FLOAT":
			originColumnType = "BINARY_FLOAT"
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "BINARY_DOUBLE":
			originColumnType = "BINARY_DOUBLE"
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "NCHAR VARYING":
			originColumnType = "NCHAR VARYING"
			buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "NCLOB":
			originColumnType = "NCLOB"
			buildInColumnType = "TEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "NUMERIC":
			originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "RAW":
			originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
			if dataLength < 256 {
				buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
			}
			buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "REAL":
			originColumnType = "real"
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "ROWID":
			originColumnType = "ROWID"
			buildInColumnType = "CHAR(10)"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "SMALLINT":
			originColumnType = "SMALLINT"
			buildInColumnType = "DECIMAL(38)"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "UROWID":
			originColumnType = "UROWID"
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "VARCHAR":
			originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		case "XMLTYPE":
			originColumnType = "XMLTYPE"
			buildInColumnType = "LONGTEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		default:
			if strings.Contains(dataType, "INTERVAL") {
				originColumnType = dataType
				buildInColumnType = "VARCHAR(30)"
			} else if strings.Contains(dataType, "TIMESTAMP") {
				originColumnType = dataType
				if dataScale == 0 {
					buildInColumnType = "DATETIME"
				}
				buildInColumnType = fmt.Sprintf("DATETIME(%s)", dataScale)
			} else {
				originColumnType = dataType
				buildInColumnType = "TEXT"
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
		}
	}
	// CHAR、VARCHAR2、NCHAR、NVARCHAR、NVARCHAR2

}
