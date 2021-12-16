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
	"reflect"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"github.com/wentaojin/transferdb/pkg/reverser"

	"go.uber.org/zap"
)

type DiffWriter struct {
	SourceSchemaName string
	TargetSchemaName string
	TableName        string
	Engine           *service.Engine
	ChkFileMW        *reverser.FileMW
	RevFileMW        *reverser.FileMW
	CompFileMW       *reverser.FileMW
}

func NewDiffWriter(sourceSchemaName, targetSchemaName, tableName string, engine *service.Engine, chkFileMW, revFileMW, compFileMW *reverser.FileMW) *DiffWriter {
	return &DiffWriter{
		SourceSchemaName: sourceSchemaName,
		TargetSchemaName: targetSchemaName,
		TableName:        tableName,
		Engine:           engine,
		ChkFileMW:        chkFileMW,
		RevFileMW:        revFileMW,
		CompFileMW:       compFileMW,
	}
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

	var builder strings.Builder

	// 判断 MySQL 表是否存在
	isExist, err := d.Engine.IsExistMySQLTable(d.TargetSchemaName, d.TableName)
	if err != nil {
		return err
	}
	if !isExist {
		// 表列表
		reverseTables, partitionTableList, temporaryTableList, clusteredTableList, err := reverser.LoadOracleToMySQLTableList(d.Engine, []string{d.TableName}, d.SourceSchemaName, d.TargetSchemaName, false)
		if err != nil {
			return err
		}

		// 不兼容项 - 表提示
		if err = reverser.CompatibilityDBTips(d.CompFileMW, d.SourceSchemaName, partitionTableList, temporaryTableList, clusteredTableList); err != nil {
			return err
		}

		for _, tbl := range reverseTables {
			writer, er := reverser.NewReverseWriter(tbl, d.RevFileMW, d.CompFileMW)
			if er != nil {
				return er
			}
			if er = writer.Reverse(); er != nil {
				return er
			}
		}
		service.Logger.Warn("table not exists",
			zap.String("oracle table", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
			zap.String("create mysql table", fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TableName)))

		return nil
	}

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

	// 表类型检查 - only 分区表
	service.Logger.Info("check table",
		zap.String("table partition type check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if oracleTable.IsPartition != mysqlTable.IsPartition {
		if err = d.partitionRuleCheck(d.ChkFileMW, oracleTable, mysqlTable); err != nil {
			return err
		}
		return nil
	}

	// 表注释检查
	service.Logger.Info("check table",
		zap.String("table comment check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if oracleTable.TableComment != mysqlTable.TableComment {
		d.commentRuleCheck(builder, oracleTable, mysqlTable)
	}

	// 表级别字符集以及排序规则检查
	service.Logger.Info("check table",
		zap.String("table character set and collation check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if !strings.Contains(mysqlTable.TableCharacterSet, utils.OracleUTF8CharacterSet) || !strings.Contains(mysqlTable.TableCollation, utils.OracleCollationBin) {
		d.tableCharacterSetRuleCheck(builder, oracleTable, mysqlTable)
	}

	// 表字段级别字符集以及排序规则校验 -> 基于原表字段类型以及字符集、排序规则
	service.Logger.Info("check table",
		zap.String("table column character set and collation check", fmt.Sprintf("%s.%s", d.TargetSchemaName, d.TableName)))
	d.columnCharacterSetRuleCheck(builder, oracleTable, mysqlTable)

	// 表主键/唯一约束检查
	service.Logger.Info("check table",
		zap.String("table pk and uk constraint check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
		zap.String("oracle struct", oracleTable.String(utils.PUConstraintJSON)),
		zap.String("mysql struct", mysqlTable.String(utils.PUConstraintJSON)))
	// 函数 utils.DiffStructArray 都忽略 structA 空，但 structB 存在情况
	if err := d.primaryAndUniqueKeyRuleCheck(builder, oracleTable, mysqlTable); err != nil {
		return err
	}

	// TiDB 版本排除外键以及检查约束检查
	if !isTiDB {
		service.Logger.Info("check table",
			zap.String("table fk constraint check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
			zap.String("oracle struct", oracleTable.String(utils.FKConstraintJSON)),
			zap.String("mysql struct", mysqlTable.String(utils.FKConstraintJSON)))

		// 外键约束检查
		if err = d.foreignKeyRuleCheck(builder, oracleTable, mysqlTable); err != nil {
			return err
		}

		var dbVersion string
		if strings.Contains(mysqlVersion, utils.MySQLVersionDelimiter) {
			dbVersion = strings.Split(mysqlVersion, utils.MySQLVersionDelimiter)[0]
		} else {
			dbVersion = mysqlVersion
		}
		if utils.VersionOrdinal(dbVersion) > utils.VersionOrdinal(utils.MySQLCheckConsVersion) {
			service.Logger.Info("check table",
				zap.String("table ck constraint check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
				zap.String("oracle struct", oracleTable.String(utils.CKConstraintJSON)),
				zap.String("mysql struct", mysqlTable.String(utils.CKConstraintJSON)))
			// 检查约束检查
			if err = d.checkKeyRuleCheck(builder, oracleTable, mysqlTable); err != nil {
				return err
			}
		}
	}

	// 索引检查
	service.Logger.Info("check table",
		zap.String("table indexes check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
		zap.String("oracle struct", oracleTable.String(utils.IndexJSON)),
		zap.String("mysql struct", mysqlTable.String(utils.IndexJSON)))
	if err = d.indexRuleCheck(builder, oracleTable, mysqlTable); err != nil {
		return err
	}

	// 分区表检查
	if mysqlTable.IsPartition && oracleTable.IsPartition {
		service.Logger.Info("check table",
			zap.String("table partition check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
			zap.String("oracle struct", oracleTable.String(utils.PartitionJSON)),
			zap.String("mysql struct", mysqlTable.String(utils.PartitionJSON)))

		addDiffParts, _, isOK := utils.DiffStructArray(oracleTable.Partitions, mysqlTable.Partitions)
		if len(addDiffParts) != 0 && !isOK {
			builder.WriteString("/*\n")
			builder.WriteString(" oracle and mysql table partitions\n")

			t := table.NewWriter()
			t.SetStyle(table.StyleLight)
			t.AppendHeader(table.Row{"TABLE", "PARTITIONS", "SUGGEST"})
			t.AppendRows([]table.Row{
				{d.TableName, "Oracle And Mysql Different", "Manual Create Partition Table"},
			})
			builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

			builder.WriteString("*/\n")
			builder.WriteString("-- oracle partition info exist, mysql partition isn't exist, please manual modify\n")

			for _, part := range addDiffParts {
				value, ok := part.(Partition)
				if ok {
					partJSON, err := json.Marshal(value)
					if err != nil {
						return err
					}
					builder.WriteString(fmt.Sprintf("# oracle partition info: %s, ", partJSON))
					continue
				}
				return fmt.Errorf("oracle table [%s] paritions [%v] assert Partition failed, type: [%v]", oracleTable.TableName, part, reflect.TypeOf(part))
			}
		}
	}

	// 表字段检查
	// 注释格式化
	service.Logger.Info("check table",
		zap.String("table column info check", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)))
	if err = d.columnRuleCheck(builder, oracleTable, mysqlTable); err != nil {
		return err
	}

	// diff 记录不为空
	if builder.String() != "" {
		if _, err := fmt.Fprintln(d.ChkFileMW, builder.String()); err != nil {
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

func (d *DiffWriter) partitionRuleCheck(file *reverser.FileMW, oracleTable, mysqlTable *Table) error {
	var builder strings.Builder
	builder.WriteString("/*\n")
	builder.WriteString(fmt.Sprintf(" oracle table type is different from mysql table type\n"))

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"TABLE", "PARTITION", "ORACLE", "MYSQL", "SUGGEST"})
	t.AppendRows([]table.Row{
		{d.TableName, "PARTITION", oracleTable.IsPartition, mysqlTable.IsPartition, "Manual Create Partition Table"},
	})
	builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
	builder.WriteString("*/\n")

	if _, err := fmt.Fprintln(file, builder.String()); err != nil {
		return err
	}

	service.Logger.Warn("table type different",
		zap.String("oracle table", fmt.Sprintf("%s.%s partition [%t]", d.SourceSchemaName, d.TableName, oracleTable.IsPartition)),
		zap.String("mysql table", fmt.Sprintf("%s.%s partition [%t]", d.TargetSchemaName, d.TableName,
			mysqlTable.IsPartition)))

	return nil
}

func (d *DiffWriter) commentRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) {
	builder.WriteString("/*\n")
	builder.WriteString(fmt.Sprintf(" oracle and mysql table comment\n"))

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"TABLE", "COMMENT", "ORACLE", "MYSQL", "SUGGEST"})
	t.AppendRows([]table.Row{
		{d.TableName, "COMMENT", oracleTable.TableComment, mysqlTable.TableComment, "Create Table Comment"},
	})
	builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

	builder.WriteString("*/\n")
	builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s COMMENT '%s';\n", d.TargetSchemaName, d.TableName, oracleTable.TableComment))
}

func (d *DiffWriter) tableCharacterSetRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) {
	builder.WriteString("/*\n")
	builder.WriteString(fmt.Sprintf(" oracle and mysql table character set and collation\n"))

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"TABLE", "CHARACTER AND COLLATION", "ORACLE", "MYSQL", "SUGGEST"})
	t.AppendRows([]table.Row{
		{d.TableName, "CHARACTER AND COLLATION",
			fmt.Sprintf("character set[%s] collation[%s]", oracleTable.TableCharacterSet, oracleTable.TableCollation),
			fmt.Sprintf("character set[%s] collation[%s]", mysqlTable.TableCharacterSet, mysqlTable.TableCollation),
			"Create Table Character Collation"},
	})
	builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

	builder.WriteString("*/\n")
	builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s CHARACTER SET = %s, COLLATE = %s;\n", d.TargetSchemaName, d.TableName, utils.MySQLCharacterSet, utils.MySQLCollation))
}

func (d *DiffWriter) columnCharacterSetRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) {
	tableColumnsMap := make(map[string]Column)
	for mysqlColName, mysqlColInfo := range mysqlTable.Columns {
		if mysqlColInfo.CharacterSet != "UNKNOWN" || mysqlColInfo.Collation != "UNKNOWN" {
			if mysqlColInfo.CharacterSet != strings.ToUpper(utils.MySQLCharacterSet) || mysqlColInfo.Collation != strings.ToUpper(utils.MySQLCollation) {
				tableColumnsMap[mysqlColName] = mysqlColInfo
			}
		}
	}

	if len(tableColumnsMap) != 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" mysql column character set and collation check, generate created sql\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COLUMN", "MYSQL", "SUGGEST"})

		var sqlStrings []string
		for mysqlColName, mysqlColInfo := range tableColumnsMap {
			t.AppendRows([]table.Row{
				{d.TableName, mysqlColName,
					fmt.Sprintf("%s(%s)", mysqlColInfo.DataType, mysqlColInfo.DataLength), "Create Table Column Character Collation"},
			})
			sqlStrings = append(sqlStrings, fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s(%s) CHARACTER SET %s COLLATE %s;",
				d.TargetSchemaName, d.TableName, mysqlColName, mysqlColInfo.DataType, mysqlColInfo.DataLength, utils.MySQLCharacterSet, utils.MySQLCollation))
		}

		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(strings.Join(sqlStrings, "\n"))
	}
}

func (d *DiffWriter) primaryAndUniqueKeyRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) error {
	addDiffPU, _, isOK := utils.DiffStructArray(oracleTable.PUConstraints, mysqlTable.PUConstraints)
	if len(addDiffPU) != 0 && !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle and mysql table primary key and unique key\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "PK AND UK", "SUGGEST"})
		t.AppendRows([]table.Row{
			{d.TableName, "Oracle And Mysql Different", "Create Table Primary And Unique Key"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		for _, pu := range addDiffPU {
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
			return fmt.Errorf("oracle table [%s] constraint primary and unique key [%v] assert ConstraintPUKey failed, type: [%v]", oracleTable.TableName, pu, reflect.TypeOf(pu))
		}
	}
	return nil
}

func (d *DiffWriter) foreignKeyRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) error {
	addDiffFK, _, isOK := utils.DiffStructArray(oracleTable.ForeignConstraints, mysqlTable.ForeignConstraints)
	if len(addDiffFK) != 0 && !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle and mysql table foreign key\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "FOREIGN KEY", "SUGGEST"})
		t.AppendRows([]table.Row{
			{d.TableName, "Oracle And Mysql Different", "Create Table Foreign Key"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")

		for _, fk := range addDiffFK {
			value, ok := fk.(ConstraintForeign)
			if ok {
				builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD FOREIGN KEY(%s) REFERENCES %s.%s(%s）ON DELETE %s;\n", d.TargetSchemaName, d.TableName, value.ColumnName, d.TargetSchemaName, value.ReferencedTableName, value.ReferencedColumnName, value.DeleteRule))
				continue
			}
			return fmt.Errorf("oracle table [%s] constraint foreign key [%v] assert ConstraintForeign failed, type: [%v]", oracleTable.TableName, fk, reflect.TypeOf(fk))
		}
	}
	return nil
}

func (d *DiffWriter) checkKeyRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) error {
	addDiffCK, _, isOK := utils.DiffStructArray(oracleTable.CheckConstraints, mysqlTable.CheckConstraints)
	if len(addDiffCK) != 0 && !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle and mysql table check key\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "CHECK KEY", "SUGGEST"})
		t.AppendRows([]table.Row{
			{d.TableName, "Oracle And Mysql Different", "Create Table Check Key"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		for _, ck := range addDiffCK {
			value, ok := ck.(ConstraintCheck)
			if ok {
				builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK(%s);\n",
					d.TargetSchemaName, d.TableName, fmt.Sprintf("%s_check_key", d.TableName), value.ConstraintExpression))
				continue
			}
			return fmt.Errorf("oracle table [%s] constraint check key [%v] assert ConstraintCheck failed, type: [%v]", oracleTable.TableName, ck, reflect.TypeOf(ck))
		}
	}
	return nil
}

func (d *DiffWriter) indexRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) error {
	var createIndexSQL []string
	addDiffIndex, _, isOK := utils.DiffStructArray(oracleTable.Indexes, mysqlTable.Indexes)
	if len(addDiffIndex) != 0 && !isOK {
		for _, idx := range addDiffIndex {
			value, ok := idx.(Index)
			if ok {
				if value.Uniqueness == "UNIQUE" && value.IndexType == "NORMAL" {
					// 考虑 MySQL 索引类型 BTREE，额外判断处理
					var equalArray []interface{}
					for _, mysqlIndexInfo := range mysqlTable.Indexes {
						if reflect.DeepEqual(value.IndexInfo, mysqlIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, d.TargetSchemaName, d.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "UNIQUE" && value.IndexType == "FUNCTION-BASED NORMAL" {
					// 考虑 MySQL 索引类型 BTREE，额外判断处理
					var equalArray []interface{}
					for _, mysqlIndexInfo := range mysqlTable.Indexes {
						if reflect.DeepEqual(value.IndexInfo, mysqlIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, d.TargetSchemaName, d.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "NORMAL" {
					// 考虑 MySQL 索引类型 BTREE，额外判断处理
					var equalArray []interface{}
					for _, mysqlIndexInfo := range mysqlTable.Indexes {
						if reflect.DeepEqual(value.IndexInfo, mysqlIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, d.TargetSchemaName, d.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "BITMAP" {
					createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);\n",
						value.IndexName, d.TargetSchemaName, d.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "FUNCTION-BASED NORMAL" {
					createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);\n",
						value.IndexName, d.TargetSchemaName, d.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "FUNCTION-BASED BITMAP" {
					createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);\n",
						value.IndexName, d.TargetSchemaName, d.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "DOMAIN" {
					createIndexSQL = append(createIndexSQL,
						fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');\n",
							value.IndexName, d.TargetSchemaName, d.TableName, value.IndexColumn,
							value.DomainIndexOwner, value.DomainIndexName, value.DomainParameters))
					continue
				}
				return fmt.Errorf("oracle table [%s] diff failed, not support index: [%v]", oracleTable.TableName, value)
			}
			return fmt.Errorf("oracle table [%s] index [%v] assert Index failed, type: [%v]", oracleTable.TableName, idx, reflect.TypeOf(idx))
		}
	}

	if len(createIndexSQL) != 0 {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle and mysql table indexes\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "INDEXES", "SUGGEST"})
		t.AppendRows([]table.Row{
			{d.TableName, "Oracle And Mysql Different", "Create Table Index"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		for _, indexSQL := range createIndexSQL {
			builder.WriteString(indexSQL)
		}
	}
	return nil
}

func (d *DiffWriter) columnRuleCheck(builder strings.Builder, oracleTable, mysqlTable *Table) error {
	var (
		diffColumnMsgs    []string
		createColumnMetas []string
		tableRowArray     []table.Row
	)

	for oracleColName, oracleColInfo := range oracleTable.Columns {
		mysqlColInfo, ok := mysqlTable.Columns[oracleColName]
		if ok {
			diffColumnMsg, tableRows, err := OracleTableMapRuleCheck(
				d.SourceSchemaName,
				d.TargetSchemaName,
				d.TableName,
				oracleColName,
				oracleColInfo,
				mysqlColInfo)
			if err != nil {
				return err
			}
			if diffColumnMsg != "" && len(tableRows) != 0 {
				diffColumnMsgs = append(diffColumnMsgs, diffColumnMsg)
				tableRowArray = append(tableRowArray, tableRows)
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
			d.Engine)
		if err != nil {
			return err
		}
		if columnMeta != "" {
			createColumnMetas = append(createColumnMetas, columnMeta)
		}
	}

	if len(tableRowArray) != 0 && len(diffColumnMsgs) != 0 {
		service.Logger.Info("check table",
			zap.String("table column info check, generate fixed sql", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
			zap.String("oracle struct", oracleTable.String(utils.ColumnsJSON)),
			zap.String("mysql struct", mysqlTable.String(utils.ColumnsJSON)))

		textTable := table.NewWriter()
		textTable.SetStyle(table.StyleLight)
		textTable.AppendHeader(table.Row{"Column", "ORACLE", "MySQL", "Suggest"})
		textTable.AppendRows(tableRowArray)

		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table columns info is different from mysql\n"))
		builder.WriteString(fmt.Sprintf("%s\n", textTable.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("-- oracle table columns info is different from mysql, generate fixed sql\n"))
		for _, diffColMsg := range diffColumnMsgs {
			builder.WriteString(diffColMsg)
		}
		builder.WriteString("\n")
	}

	if len(createColumnMetas) != 0 {
		service.Logger.Info("check table",
			zap.String("table column info check, generate created sql", fmt.Sprintf("%s.%s", d.SourceSchemaName, d.TableName)),
			zap.String("oracle struct", oracleTable.String(utils.ColumnsJSON)),
			zap.String("mysql struct", mysqlTable.String(utils.ColumnsJSON)))

		builder.WriteString(fmt.Sprintf("-- oracle table columns info isn't exist in mysql, generate created sql\n"))
		for _, columnMeta := range createColumnMetas {
			builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s;\n",
				d.TargetSchemaName, d.TableName, columnMeta))
		}
	}
	return nil
}
