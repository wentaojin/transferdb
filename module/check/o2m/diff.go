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
	"context"
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/module/check"
	"go.uber.org/zap"
	"reflect"
	"strings"
	"time"
)

type Check struct {
	Ctx             context.Context
	OracleTableINFO *Table     `json:"oracle_table_info"`
	MySQLTableINFO  *Table     `json:"mysql_table_info"`
	MySQLDBVersion  string     `json:"mysqldb_version"`
	MySQLDBType     string     `json:"mysqldb_type"`
	MetaDB          *meta.Meta `json:"-"`
}

func NewChecker(ctx context.Context, oracleTableInfo, mysqlTableInfo *Table, mysqlDBVersion, targetDBType string, metaDB *meta.Meta) *Check {
	return &Check{
		Ctx:             ctx,
		OracleTableINFO: oracleTableInfo,
		MySQLTableINFO:  mysqlTableInfo,
		MySQLDBVersion:  mysqlDBVersion,
		MySQLDBType:     targetDBType,
		MetaDB:          metaDB,
	}
}

// 表结构对比
// 以上游 oracle 表结构信息为基准，对比下游 MySQL 表结构
// 1、若上游存在，下游不存在，则输出记录，若上游不存在，下游存在，则默认不输出
// 2、忽略上下游不同索引名、约束名对比，只对比下游是否存在同等约束下同等字段是否存在
// 3、分区只对比分区类型、分区键、分区表达式等，不对比具体每个分区下的情况

func (c *Check) CheckPartitionTableType() string {
	// 表类型检查 - only 分区表
	zap.L().Info("check table",
		zap.String("table partition type check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)))

	var builder strings.Builder
	if c.OracleTableINFO.IsPartition != c.MySQLTableINFO.IsPartition {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table type is different from mysql table type\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "PARTITION", "ORACLE", "MYSQL", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.OracleTableINFO.TableName, "PARTITION", c.OracleTableINFO.IsPartition, c.MySQLTableINFO.IsPartition, "Manual Create Partition Table"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")

		zap.L().Warn("table type different",
			zap.String("oracle table", fmt.Sprintf("%s.%s partition [%t]", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableCharacterSet, c.OracleTableINFO.IsPartition)),
			zap.String("mysql table", fmt.Sprintf("%s.%s partition [%t]", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, c.MySQLTableINFO.IsPartition)))
	}
	return builder.String()

}

func (c *Check) CheckTableComment() string {
	// 表注释检查
	zap.L().Info("check table",
		zap.String("table comment check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)))

	var builder strings.Builder
	if !strings.EqualFold(c.OracleTableINFO.TableComment, c.MySQLTableINFO.TableComment) {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle and mysql table comment\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COMMENT", "ORACLE", "MYSQL", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.OracleTableINFO.TableName, "COMMENT", c.OracleTableINFO.TableComment, c.MySQLTableINFO.TableComment, "Create Table Comment"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s COMMENT '%s';\n", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, c.OracleTableINFO.TableComment))
	}
	return builder.String()
}

func (c *Check) CheckTableCharacterSetAndCollation() string {
	// 表级别字符集以及排序规则检查
	zap.L().Info("check table",
		zap.String("table character set and collation check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)))
	// GBK 处理，统一 UTF8MB4 处理
	var oracleCharacterSet string
	if common.OracleDBCharacterSetMap[c.OracleTableINFO.TableCharacterSet] == "GBK" {
		oracleCharacterSet = common.MySQLCharacterSet
	} else {
		oracleCharacterSet = common.OracleDBCharacterSetMap[c.OracleTableINFO.TableCharacterSet]
	}

	var builder strings.Builder

	if c.MySQLTableINFO.TableCharacterSet != oracleCharacterSet || c.MySQLTableINFO.TableCollation != strings.ToUpper(common.OracleCollationMap[c.OracleTableINFO.TableCollation]) {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle and mysql table character set and collation\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "CHARACTER AND COLLATION", "ORACLE", "MYSQL", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.OracleTableINFO.TableName, "CHARACTER AND COLLATION",
				fmt.Sprintf("character set [%s] collation [%s]", c.OracleTableINFO.TableCharacterSet, c.OracleTableINFO.TableCollation),
				fmt.Sprintf("character set [%s] collation [%s]", c.MySQLTableINFO.TableCharacterSet, c.MySQLTableINFO.TableCollation),
				"Create Table Character Collation"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")

		// GBK 处理，统一 UTF8MB4 处理
		var mysqlCharacterSet string
		if strings.ToUpper(common.OracleDBCharacterSetMap[c.OracleTableINFO.TableCharacterSet]) == "GBK" {
			mysqlCharacterSet = "UTF8MB4"
			zap.L().Warn("check oracle table",
				zap.String("schema", c.OracleTableINFO.SchemaName),
				zap.String("table", c.OracleTableINFO.TableName),
				zap.String("characterSet", c.OracleTableINFO.TableCharacterSet),
				zap.String("msg", "GBK TO UTF8MB4"))
		} else {
			mysqlCharacterSet = common.OracleDBCharacterSetMap[c.OracleTableINFO.TableCharacterSet]
		}
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s CHARACTER SET %s COLLATE %s;\n\n", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName,
			strings.ToLower(mysqlCharacterSet),
			strings.ToLower(common.OracleCollationMap[c.OracleTableINFO.TableCollation])))
	}

	return builder.String()
}

func (c *Check) CheckColumnCharacterSetAndCollation() string {
	// 1、表字段级别字符集以及排序规则校验 -> 基于原表字段类型以及字符集、排序规则
	// 2、下游表字段数检查多了
	zap.L().Info("check table",
		zap.String("table column character set and collation check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)))

	var builder strings.Builder

	tableColumnsMap := make(map[string]Column)
	delColumnsMap := make(map[string]Column)

	for mysqlColName, mysqlColInfo := range c.MySQLTableINFO.Columns {
		if _, ok := c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)]; ok {
			if mysqlColInfo.CharacterSet != "UNKNOWN" || mysqlColInfo.Collation != "UNKNOWN" {
				// GBK 处理，统一视作 UTF8MB4 处理
				var oracleCharacterSet string
				if strings.ToUpper(common.OracleDBCharacterSetMap[c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)].CharacterSet]) == "GBK" {
					oracleCharacterSet = common.MySQLCharacterSet
				} else {
					oracleCharacterSet = strings.ToUpper(common.OracleDBCharacterSetMap[c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)].CharacterSet])
				}
				if mysqlColInfo.CharacterSet != oracleCharacterSet || mysqlColInfo.Collation !=
					strings.ToUpper(common.OracleCollationMap[c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)].Collation]) {
					tableColumnsMap[mysqlColName] = mysqlColInfo
				}
			}
		} else {
			delColumnsMap[mysqlColName] = mysqlColInfo
		}
	}

	if len(tableColumnsMap) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" mysql column character set and collation modify, generate created sql\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COLUMN", "MYSQL", "SUGGEST"})

		var sqlStrings []string
		for mysqlColName, mysqlColInfo := range tableColumnsMap {
			t.AppendRows([]table.Row{
				{c.OracleTableINFO.TableName, mysqlColName,
					fmt.Sprintf("%s(%s)", mysqlColInfo.DataType, mysqlColInfo.DataLength), "Create Table Column Character Collation"},
			})

			// GBK 处理，统一 UTF8MB4 处理
			var mysqlCharacterSet string
			if strings.ToUpper(common.OracleDBCharacterSetMap[c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)].CharacterSet]) == "GBK" {
				mysqlCharacterSet = "UTF8MB4"
				zap.L().Warn("check oracle table",
					zap.String("schema", c.OracleTableINFO.SchemaName),
					zap.String("table", c.OracleTableINFO.TableName),
					zap.String("column", mysqlColName),
					zap.String("characterSet", c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)].CharacterSet),
					zap.String("msg", "GBK TO UTF8MB4"))
			} else {
				mysqlCharacterSet = common.OracleDBCharacterSetMap[c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)].CharacterSet]
			}
			sqlStrings = append(sqlStrings, fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s(%s) CHARACTER SET %s COLLATE %s;",
				c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, mysqlColName, mysqlColInfo.DataType, mysqlColInfo.DataLength,
				strings.ToLower(mysqlCharacterSet),
				strings.ToLower(common.OracleCollationMap[c.OracleTableINFO.Columns[strings.ToUpper(mysqlColName)].Collation])))
		}

		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(strings.Join(sqlStrings, "\n") + "\n\n")
	}

	if len(delColumnsMap) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" mysql column character set and collation drop [oracle column isn't exist], generate drop sql\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COLUMN", "MYSQL", "SUGGEST"})

		var sqlStrings []string
		for mysqlColName, mysqlColInfo := range delColumnsMap {
			// TIMESTAMP/DATETIME 时间字段特殊处理
			// 数据类型内自带精度
			if (strings.Contains(strings.ToUpper(mysqlColInfo.DataType), "TIMESTAMP")) || strings.Contains(strings.ToUpper(mysqlColInfo.DataType), "DATETIME") {
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, mysqlColName,
						fmt.Sprintf("%s(%s)", mysqlColInfo.DataType, mysqlColInfo.DatetimePrecision), "Drop MySQL Table Column"},
				})
			} else {
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, mysqlColName,
						fmt.Sprintf("%s(%s)", mysqlColInfo.DataType, mysqlColInfo.DataLength), "Drop MySQL Table Column"},
				})
			}

			sqlStrings = append(sqlStrings, fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s;", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, mysqlColName))
		}

		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(strings.Join(sqlStrings, "\n") + "\n\n")
	}
	return builder.String()
}

func (c *Check) CheckColumnCounts() (string, error) {
	// 上游表字段数检查
	zap.L().Info("check table",
		zap.String("oracle table column counts check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)))

	var builder strings.Builder

	addColumnsMap := make(map[string]Column)

	for oracleColName, oracleColInfo := range c.OracleTableINFO.Columns {
		if _, ok := c.MySQLTableINFO.Columns[strings.ToUpper(oracleColName)]; !ok {
			addColumnsMap[oracleColName] = oracleColInfo
		}
	}
	if len(addColumnsMap) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" mysql column character set and collation add [mysql column isn't exist], generate add sql\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COLUMN", "ORACLE", "SUGGEST"})

		var sqlStrings []string
		for oracleColName, oracleColInfo := range addColumnsMap {
			var (
				columnMeta string
				err        error
			)
			columnMeta, err = GenOracleTableColumnMeta(c.Ctx, c.MetaDB, c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, oracleColName, oracleColInfo)
			if err != nil {
				return columnMeta, err
			}
			// TIMESTAMP 时间字段特殊处理
			// 数据类型内自带精度
			if strings.Contains(strings.ToUpper(oracleColInfo.DataType), "TIMESTAMP") {
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, oracleColName,
						fmt.Sprintf("%s", oracleColInfo.DataType), "Add MySQL Table Column"},
				})
			} else {
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, oracleColName,
						fmt.Sprintf("%s(%s)", oracleColInfo.DataType, oracleColInfo.DataLength), "Add MySQL Table Column"},
				})
			}
			sqlStrings = append(sqlStrings, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s;", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, columnMeta))
		}

		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(strings.Join(sqlStrings, "\n") + "\n\n")
	}

	return builder.String(), nil
}

func (c *Check) CheckPrimaryAndUniqueKey() (string, error) {
	// 表主键/唯一约束检查
	zap.L().Info("check table",
		zap.String("table pk and uk constraint check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
		zap.String("oracle struct", c.OracleTableINFO.String(common.PUConstraintJSON)),
		zap.String("mysql struct", c.MySQLTableINFO.String(common.PUConstraintJSON)))
	// 函数 utils.DiffStructArray 都忽略 structA 空，但 structB 存在情况
	addDiffPU, _, isOK := common.DiffStructArray(c.OracleTableINFO.PUConstraints, c.MySQLTableINFO.PUConstraints)

	var builder strings.Builder

	if len(addDiffPU) != 0 && !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle and mysql table primary key and unique key\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "PK AND UK", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.OracleTableINFO.TableName, "Oracle And Mysql Different", "Create Table Primary And Unique Key"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		for _, pu := range addDiffPU {
			value, ok := pu.(ConstraintPUKey)
			if ok {
				switch value.ConstraintType {
				case "PK":
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY(%s);\n", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.ConstraintColumn))
					continue
				case "UK":
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD UNIQUE(%s);\n", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.ConstraintColumn))
					continue
				default:
					return builder.String(), fmt.Errorf("table constraint primary and unique key diff failed: not support type [%s]", value.ConstraintType)
				}
			}
			return builder.String(), fmt.Errorf("oracle table [%s] constraint primary and unique key [%v] assert ConstraintPUKey failed, type: [%v]", c.OracleTableINFO.TableName, pu, reflect.TypeOf(pu))
		}
	}
	return builder.String(), nil
}

func (c *Check) CheckForeignKey() (string, error) {
	isTiDB := false
	if strings.ToUpper(c.MySQLDBType) == common.TiDBTargetDBType {
		isTiDB = true
	}
	var builder strings.Builder
	// TiDB 版本排除外键以及检查约束检查
	if !isTiDB {
		zap.L().Info("check table",
			zap.String("table fk constraint check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
			zap.String("oracle struct", c.OracleTableINFO.String(common.FKConstraintJSON)),
			zap.String("mysql struct", c.MySQLTableINFO.String(common.FKConstraintJSON)))

		// 外键约束检查
		addDiffFK, _, isOK := common.DiffStructArray(c.OracleTableINFO.ForeignConstraints, c.MySQLTableINFO.ForeignConstraints)
		if len(addDiffFK) != 0 && !isOK {
			builder.WriteString("/*\n")
			builder.WriteString(" oracle and mysql table foreign key\n")

			t := table.NewWriter()
			t.SetStyle(table.StyleLight)
			t.AppendHeader(table.Row{"TABLE", "FOREIGN KEY", "SUGGEST"})
			t.AppendRows([]table.Row{
				{c.OracleTableINFO.TableName, "Oracle And Mysql Different", "Create Table Foreign Key"},
			})
			builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

			builder.WriteString("*/\n")

			for _, fk := range addDiffFK {
				value, ok := fk.(ConstraintForeign)
				if ok {
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD FOREIGN KEY(%s) REFERENCES %s.%s(%s）ON DELETE %s;\n", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.ColumnName, c.MySQLTableINFO.SchemaName, value.ReferencedTableName, value.ReferencedColumnName, value.DeleteRule))
					continue
				}
				return builder.String(), fmt.Errorf("oracle table [%s] constraint foreign key [%v] assert ConstraintForeign failed, type: [%v]", c.OracleTableINFO.TableName, fk, reflect.TypeOf(fk))
			}
		}
	}
	return builder.String(), nil
}

func (c *Check) CheckCheckKey() (string, error) {
	isTiDB := false
	if strings.ToUpper(c.MySQLDBType) == common.TiDBTargetDBType {
		isTiDB = true
	}
	var builder strings.Builder

	// TiDB 版本排除外键以及检查约束检查
	if !isTiDB {
		var dbVersion string
		if strings.Contains(c.MySQLDBVersion, common.MySQLVersionDelimiter) {
			dbVersion = strings.Split(c.MySQLDBVersion, common.MySQLVersionDelimiter)[0]
		} else {
			dbVersion = c.MySQLDBVersion
		}
		if common.VersionOrdinal(dbVersion) > common.VersionOrdinal(common.MySQLCheckConsVersion) {
			zap.L().Info("check table",
				zap.String("table ck constraint check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
				zap.String("oracle struct", c.OracleTableINFO.String(common.CKConstraintJSON)),
				zap.String("mysql struct", c.MySQLTableINFO.String(common.CKConstraintJSON)))
			// 检查约束检查
			addDiffCK, _, isOK := common.DiffStructArray(c.OracleTableINFO.CheckConstraints, c.MySQLTableINFO.CheckConstraints)
			if len(addDiffCK) != 0 && !isOK {
				builder.WriteString("/*\n")
				builder.WriteString(" oracle and mysql table check key\n")

				t := table.NewWriter()
				t.SetStyle(table.StyleLight)
				t.AppendHeader(table.Row{"TABLE", "CHECK KEY", "SUGGEST"})
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, "Oracle And Mysql Different", "Create Table Check Key"},
				})
				builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

				builder.WriteString("*/\n")
				for _, ck := range addDiffCK {
					value, ok := ck.(ConstraintCheck)
					if ok {
						builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK(%s);\n", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, fmt.Sprintf("%s_check_key", c.MySQLTableINFO.TableName), value.ConstraintExpression))
						continue
					}
					return builder.String(), fmt.Errorf("oracle table [%s] constraint check key [%v] assert ConstraintCheck failed, type: [%v]", c.OracleTableINFO.TableName, ck, reflect.TypeOf(ck))
				}
			}
		}
	}
	return builder.String(), nil
}

func (c *Check) CheckIndex() (string, error) {
	// 索引检查
	zap.L().Info("check table",
		zap.String("table indexes check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
		zap.String("oracle struct", c.OracleTableINFO.String(common.IndexJSON)),
		zap.String("mysql struct", c.MySQLTableINFO.String(common.IndexJSON)))

	var builder strings.Builder
	var createIndexSQL []string
	addDiffIndex, _, isOK := common.DiffStructArray(c.OracleTableINFO.Indexes, c.MySQLTableINFO.Indexes)
	if len(addDiffIndex) != 0 && !isOK {
		for _, idx := range addDiffIndex {
			value, ok := idx.(Index)
			if ok {
				if value.Uniqueness == "UNIQUE" && value.IndexType == "NORMAL" {
					// 考虑 MySQL 索引类型 BTREE，额外判断处理
					var equalArray []interface{}
					for _, mysqlIndexInfo := range c.MySQLTableINFO.Indexes {
						if reflect.DeepEqual(value.IndexInfo, mysqlIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "UNIQUE" && value.IndexType == "FUNCTION-BASED NORMAL" {
					// 考虑 MySQL 索引类型 BTREE，额外判断处理
					var equalArray []interface{}
					for _, mysqlIndexInfo := range c.MySQLTableINFO.Indexes {
						if reflect.DeepEqual(value.IndexInfo, mysqlIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "NORMAL" {
					// 考虑 MySQL 索引类型 BTREE，额外判断处理
					var equalArray []interface{}
					for _, mysqlIndexInfo := range c.MySQLTableINFO.Indexes {
						if reflect.DeepEqual(value.IndexInfo, mysqlIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "BITMAP" {
					createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);\n",
						value.IndexName, c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "FUNCTION-BASED NORMAL" {
					createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);\n",
						value.IndexName, c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "FUNCTION-BASED BITMAP" {
					createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);\n",
						value.IndexName, c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.IndexColumn))
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "DOMAIN" {
					createIndexSQL = append(createIndexSQL,
						fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');\n",
							value.IndexName, c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, value.IndexColumn,
							value.DomainIndexOwner, value.DomainIndexName, value.DomainParameters))
					continue
				}
				return builder.String(), fmt.Errorf("oracle table [%s] diff failed, not support index: [%v]", c.OracleTableINFO.TableName, value)
			}
			return builder.String(), fmt.Errorf("oracle table [%s] index [%v] assert Index failed, type: [%v]", c.OracleTableINFO.TableName, idx, reflect.TypeOf(idx))
		}
	}

	if len(createIndexSQL) != 0 {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle and mysql table indexes\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "INDEXES", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.OracleTableINFO.TableName, "Oracle And Mysql Different", "Create Table Index"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		for _, indexSQL := range createIndexSQL {
			builder.WriteString(indexSQL)
		}
	}

	return builder.String(), nil
}

func (c *Check) CheckPartitionTable() (string, error) {
	// 分区表检查
	var builder strings.Builder
	if c.MySQLTableINFO.IsPartition && c.OracleTableINFO.IsPartition {
		zap.L().Info("check table",
			zap.String("table partition check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
			zap.String("oracle struct", c.OracleTableINFO.String(common.PartitionJSON)),
			zap.String("mysql struct", c.MySQLTableINFO.String(common.PartitionJSON)))

		addDiffParts, _, isOK := common.DiffStructArray(c.OracleTableINFO.Partitions, c.MySQLTableINFO.Partitions)
		if len(addDiffParts) != 0 && !isOK {
			builder.WriteString("/*\n")
			builder.WriteString(" oracle and mysql table partitions\n")

			t := table.NewWriter()
			t.SetStyle(table.StyleLight)
			t.AppendHeader(table.Row{"TABLE", "PARTITIONS", "SUGGEST"})
			t.AppendRows([]table.Row{
				{c.OracleTableINFO.TableName, "Oracle And Mysql Different", "Manual Create Partition Table"},
			})
			builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

			builder.WriteString("*/\n")
			builder.WriteString("-- oracle partition info exist, mysql partition isn't exist, please manual modify\n")

			for _, part := range addDiffParts {
				value, ok := part.(Partition)
				if ok {
					partJSON, err := json.Marshal(value)
					if err != nil {
						return builder.String(), err
					}
					builder.WriteString(fmt.Sprintf("# oracle partition info: %s, ", partJSON))
					continue
				}
				return builder.String(), fmt.Errorf("oracle table [%s] paritions [%v] assert Partition failed, type: [%v]", c.OracleTableINFO.TableName, part, reflect.TypeOf(part))
			}
		}
	}
	return builder.String(), nil
}

func (c *Check) CheckColumn() (string, error) {
	// 表字段检查
	// 注释格式化
	zap.L().Info("check table",
		zap.String("table column info check", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)))

	var (
		diffColumnMsgs []string
		tableRowArray  []table.Row
		builder        strings.Builder
	)

	for oracleColName, oracleColInfo := range c.OracleTableINFO.Columns {
		mysqlColInfo, ok := c.MySQLTableINFO.Columns[oracleColName]
		if ok {
			diffColumnMsg, tableRows, err := OracleTableColumnMapRuleCheck(
				common.StringUPPER(c.OracleTableINFO.SchemaName),
				common.StringUPPER(c.MySQLTableINFO.SchemaName),
				common.StringUPPER(c.OracleTableINFO.TableName),
				common.StringsBuilder("`", oracleColName, "`"),
				oracleColInfo,
				mysqlColInfo)
			if err != nil {
				return builder.String(), err
			}
			if diffColumnMsg != "" && len(tableRows) != 0 {
				diffColumnMsgs = append(diffColumnMsgs, diffColumnMsg)
				tableRowArray = append(tableRowArray, tableRows)
			}
			continue
		}
		// 如果源端字段不存在,则目标段字段忽略，功能与 OracleTableColumnMapRuleCheck 函数相同，对于源端存在目标端不存在的新增
	}

	if len(tableRowArray) != 0 && len(diffColumnMsgs) != 0 {
		zap.L().Info("check table",
			zap.String("table column info check, generate fixed sql", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
			zap.String("oracle struct", c.OracleTableINFO.String(common.ColumnsJSON)),
			zap.String("mysql struct", c.MySQLTableINFO.String(common.ColumnsJSON)))

		textTable := table.NewWriter()
		textTable.SetStyle(table.StyleLight)
		textTable.AppendHeader(table.Row{"Table", "Column", "ORACLE", "MySQL", "Suggest"})
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

	return builder.String(), nil
}

func (c *Check) Writer(f *check.File) error {
	startTime := time.Now()
	zap.L().Info("check table start",
		zap.String("oracle table", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
		zap.String("mysql table", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

	var builder strings.Builder

	if !strings.EqualFold(c.CheckPartitionTableType(), "") {
		builder.WriteString(c.CheckPartitionTableType())
	}
	if !strings.EqualFold(c.CheckTableComment(), "") {
		builder.WriteString(c.CheckTableComment())
	}
	if !strings.EqualFold(c.CheckTableCharacterSetAndCollation(), "") {
		builder.WriteString(c.CheckTableCharacterSetAndCollation())
	}

	counts, err := c.CheckColumnCounts()
	if err != nil {
		return err
	}
	if !strings.EqualFold(counts, "") {
		builder.WriteString(counts)
	}
	key, err := c.CheckPrimaryAndUniqueKey()
	if err != nil {
		return err
	}
	if !strings.EqualFold(key, "") {
		builder.WriteString(key)
	}
	foreignKey, err := c.CheckForeignKey()
	if err != nil {
		return err
	}
	if !strings.EqualFold(foreignKey, "") {
		builder.WriteString(foreignKey)
	}
	checkKey, err := c.CheckCheckKey()
	if err != nil {
		return err
	}
	if !strings.EqualFold(checkKey, "") {
		builder.WriteString(checkKey)
	}
	index, err := c.CheckIndex()
	if err != nil {
		return err
	}
	if !strings.EqualFold(index, "") {
		builder.WriteString(index)
	}

	partitionTable, err := c.CheckPartitionTable()
	if err != nil {
		return err
	}
	if !strings.EqualFold(partitionTable, "") {
		builder.WriteString(partitionTable)
	}

	column, err := c.CheckColumn()
	if err != nil {
		return err
	}
	if !strings.EqualFold(column, "") {
		builder.WriteString(column)
	}
	// diff 记录不为空
	if builder.String() != "" {
		if _, err := f.CWriteString(builder.String()); err != nil {
			return err
		}
	}

	endTime := time.Now()
	zap.L().Info("check table finished",
		zap.String("oracle table", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
		zap.String("mysql table", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func (c *Check) String() string {
	jsonStr, _ := json.Marshal(c)
	return string(jsonStr)
}
