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
package t2o

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/module/check"
	"github.com/wentaojin/transferdb/module/check/mysql/public"
	"go.uber.org/zap"
	"reflect"
	"strings"
	"time"
)

type Diff struct {
	Ctx                context.Context
	DBTypeS            string        `json:"db_type_s"`
	DBTypeT            string        `json:"db_type_t"`
	OracleTableINFO    *public.Table `json:"oracle_table_info"`
	MySQLTableINFO     *public.Table `json:"mysql_table_info"`
	MySQLDBVersion     string        `json:"mysqldb_version"`
	OracleDBVersion    string        `json:"oracle_db_version"`
	OracleDBExtendMode bool          `json:"oracle_db_extend_mode"`
	MetaDB             *meta.Meta    `json:"-"`
}

func NewChecker(ctx context.Context, oracleTableInfo, mysqlTableInfo *public.Table, dbTypeS, dbTypeT, mysqlDBVersion, oracleDBVersion string, oracleDBExtendMode bool, metaDB *meta.Meta) *Diff {
	return &Diff{
		Ctx:                ctx,
		DBTypeS:            dbTypeS,
		DBTypeT:            dbTypeT,
		OracleTableINFO:    oracleTableInfo,
		MySQLTableINFO:     mysqlTableInfo,
		MySQLDBVersion:     mysqlDBVersion,
		OracleDBVersion:    oracleDBVersion,
		OracleDBExtendMode: oracleDBExtendMode,
		MetaDB:             metaDB,
	}
}

// 表结构对比
// 以上游表结构信息为基准，对比下游表结构
// 1、若上游存在，下游不存在，则输出记录，若上游不存在，下游存在，则默认不输出
// 2、忽略上下游不同索引名、约束名对比，只对比下游是否存在同等约束下同等字段是否存在
// 3、分区只对比分区类型、分区键、分区表达式等，不对比具体每个分区下的情况
func (c *Diff) CheckPartitionTableType() string {
	// 表类型检查 - only 分区表
	zap.L().Info("check table",
		zap.String("table partition type check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

	var builder strings.Builder
	if c.MySQLTableINFO.IsPartition != c.OracleTableINFO.IsPartition {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" tidb table type is different from oracle table type\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "PARTITION", "TIDB", "ORACLE", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.MySQLTableINFO.TableName, "PARTITION", c.MySQLTableINFO.IsPartition, c.OracleTableINFO.IsPartition, "Manual Create Partition Table"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")

		zap.L().Warn("table type different",
			zap.String("oracle table", fmt.Sprintf("%s.%s partition [%t]", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableCharacterSet, c.OracleTableINFO.IsPartition)),
			zap.String("tidb table", fmt.Sprintf("%s.%s partition [%t]", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName, c.MySQLTableINFO.IsPartition)))
	}
	return builder.String()

}

func (c *Diff) CheckTableComment() string {
	// 表注释检查
	zap.L().Info("check table",
		zap.String("table comment check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

	var builder strings.Builder
	if !strings.EqualFold(c.MySQLTableINFO.TableComment, c.OracleTableINFO.TableComment) {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" tidb and oracle table comment\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COMMENT", "TIDB", "ORACLE", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.MySQLTableINFO.TableName, "COMMENT", c.MySQLTableINFO.TableComment, c.OracleTableINFO.TableComment, "Create Table Comment"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("COMMENT ON TABLE %s.%s IS '%s';\n", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, c.MySQLTableINFO.TableComment))
	}
	return builder.String()
}

func (c *Diff) CheckTableCharacterSetAndCollation() string {
	// 表级别字符集以及排序规则检查
	zap.L().Info("check table",
		zap.String("table character set and collation check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

	// 统一 AL32UTF8 处理
	oracleTableCharset := common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][c.MySQLTableINFO.TableCharacterSet]
	oracleTableCollations := strings.Split(common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][c.MySQLTableINFO.TableCollation][oracleTableCharset], "/")

	var builder strings.Builder

	if !strings.EqualFold(c.OracleTableINFO.TableCharacterSet, oracleTableCharset) || !common.IsContainString(oracleTableCollations, c.OracleTableINFO.TableCollation) {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" tidb and oracle table character set and collation\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "CHARACTER AND COLLATION", "TIDB", "ORACLE", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.MySQLTableINFO.TableName, "CHARACTER AND COLLATION",
				fmt.Sprintf("character set [%s] collation [%s]", c.MySQLTableINFO.TableCharacterSet, c.MySQLTableINFO.TableCollation),
				fmt.Sprintf("character set [%s] collation [%s]", c.OracleTableINFO.TableCharacterSet, c.OracleTableINFO.TableCollation),
				"Create Table Character Collation"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")

		// 统一 AL32UTF8 处理, 取第一个 collation
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s CHARACTER SET %s COLLATE %s;\n\n", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName,
			oracleTableCharset,
			oracleTableCollations[0]))
	}

	return builder.String()
}

func (c *Diff) CheckColumnCharacterSetAndCollation() string {
	// 1、表字段级别字符集以及排序规则校验 -> 基于原表字段类型以及字符集、排序规则
	// 2、下游表字段数检查多了
	zap.L().Info("check table",
		zap.String("table column character set and collation check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

	var builder strings.Builder

	tableColumnsMap := make(map[string]public.Column)
	delColumnsMap := make(map[string]public.Column)

	for oraColName, oraColInfo := range c.OracleTableINFO.Columns {
		if _, ok := c.MySQLTableINFO.Columns[strings.ToUpper(oraColName)]; ok {
			if oraColInfo.CharacterSet != "UNKNOWN" || oraColInfo.Collation != "UNKNOWN" {
				// 统一视作 AL32UTF8 处理
				oracleColumnCharset := common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][c.MySQLTableINFO.Columns[strings.ToUpper(oraColName)].CharacterSet]
				oracleColumnCollations := strings.Split(common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][c.MySQLTableINFO.Columns[strings.ToUpper(oraColName)].Collation][oracleColumnCharset], "/")

				if !strings.EqualFold(oraColInfo.CharacterSet, oracleColumnCharset) || !common.IsContainString(oracleColumnCollations, oraColInfo.Collation) {
					tableColumnsMap[oraColName] = oraColInfo
				}
			}
		} else {
			delColumnsMap[oraColName] = oraColInfo
		}
	}

	if len(tableColumnsMap) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle column character set and collation modify, generate created sql\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COLUMN", "ORACLE", "SUGGEST"})

		var sqlStrings []string
		for oraColName, oraColInfo := range tableColumnsMap {
			t.AppendRows([]table.Row{
				{c.MySQLTableINFO.TableName, oraColName,
					fmt.Sprintf("%s(%s)", oraColInfo.DataType, oraColInfo.DataLength), "Create Table Column Character Collation"},
			})

			// 统一 UTF8MB4 处理
			oracleColumnCharset := common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][c.MySQLTableINFO.Columns[strings.ToUpper(oraColName)].CharacterSet]
			oracleColumnCollations := strings.Split(common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][c.MySQLTableINFO.Columns[strings.ToUpper(oraColName)].Collation][oracleColumnCharset], "/")

			// 取第一个 collation
			sqlStrings = append(sqlStrings, fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s(%s) CHARACTER SET %s COLLATE %s;",
				c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, oraColName, oraColInfo.DataType, oraColInfo.DataLength,
				oracleColumnCharset,
				oracleColumnCollations[0]))
		}

		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(strings.Join(sqlStrings, "\n") + "\n\n")
	}

	if len(delColumnsMap) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle column character set and collation drop [mysql column isn't exist], generate add sql\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COLUMN", "ORACLE", "SUGGEST"})

		var sqlStrings []string
		for oraColName, oraColInfo := range delColumnsMap {
			// TIMESTAMP/DATETIME 时间字段特殊处理
			// 数据类型内自带精度
			if strings.Contains(strings.ToUpper(oraColInfo.DataType), "TIMESTAMP") {
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, oraColName,
						fmt.Sprintf("%s(%s)", oraColInfo.DataType, oraColInfo.DataLength), "Drop Oracle Table Column"},
				})
			} else if strings.Contains(strings.ToUpper(oraColInfo.DataType), "DATE") {
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, oraColName,
						fmt.Sprintf("%s(%s)", oraColInfo.DataType, oraColInfo.DataLength), "Drop Oracle Table Column"},
				})
			} else {
				t.AppendRows([]table.Row{
					{c.OracleTableINFO.TableName, oraColName,
						fmt.Sprintf("%s(%s)", oraColInfo.DataType, oraColInfo.DataLength), "Drop Oracle Table Column"},
				})
			}

			sqlStrings = append(sqlStrings, fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s;", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, oraColName))
		}

		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(strings.Join(sqlStrings, "\n") + "\n\n")
	}
	return builder.String()
}

func (c *Diff) CheckColumnCounts() (string, error) {
	// 上游表字段数检查
	zap.L().Info("check table",
		zap.String("tidb table column counts check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

	var builder strings.Builder

	addColumnsMap := make(map[string]public.Column)

	for oracleColName, oracleColInfo := range c.MySQLTableINFO.Columns {
		if _, ok := c.OracleTableINFO.Columns[strings.ToUpper(oracleColName)]; !ok {
			addColumnsMap[oracleColName] = oracleColInfo
		}
	}
	if len(addColumnsMap) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle column character set and collation add [oracle column isn't exist], generate add sql\n"))

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "COLUMN", "TIDB", "SUGGEST"})

		var sqlStrings []string
		for mysqlColName, mysqlColInfo := range addColumnsMap {
			var (
				columnMeta string
				err        error
			)
			columnMeta, err = public.GenOracleTableColumnMeta(c.Ctx, c.MetaDB, c.DBTypeS, c.DBTypeT, c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, mysqlColName, c.OracleDBVersion, mysqlColInfo, c.OracleDBExtendMode)
			if err != nil {
				return columnMeta, err
			}

			t.AppendRows([]table.Row{
				{c.MySQLTableINFO.TableName, mysqlColName,
					fmt.Sprintf("%s(%s)", mysqlColInfo.DataType, mysqlColInfo.DataLength), "Add Oracle Table Column"},
			})

			sqlStrings = append(sqlStrings, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s;", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, columnMeta))
		}

		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(strings.Join(sqlStrings, "\n") + "\n\n")
	}

	return builder.String(), nil
}

func (c *Diff) CheckPrimaryAndUniqueKey() (string, error) {
	// 表主键/唯一约束检查
	zap.L().Info("check table",
		zap.String("table pk and uk constraint check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)),
		zap.String("oracle struct", c.OracleTableINFO.String(common.JSONPUConstraint)),
		zap.String("tidb struct", c.MySQLTableINFO.String(common.JSONPUConstraint)))
	// 函数 utils.DiffStructArray 都忽略 structA 空，但 structB 存在情况
	addDiffPU, _, isOK := common.DiffStructArray(c.MySQLTableINFO.PUConstraints, c.OracleTableINFO.PUConstraints)

	var builder strings.Builder

	if len(addDiffPU) != 0 && !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" tidb and oracle table primary key and unique key\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "PK AND UK", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.MySQLTableINFO.TableName, "TiDB And Oracle Different", "Create Table Primary And Unique Key"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		for _, pu := range addDiffPU {
			value, ok := pu.(public.ConstraintPUKey)
			if ok {
				switch value.ConstraintType {
				case "PK":
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY(%s);\n", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, value.ConstraintColumn))
					continue
				case "UK":
					builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD UNIQUE(%s);\n", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, value.ConstraintColumn))
					continue
				default:
					return builder.String(), fmt.Errorf("table constraint primary and unique key diff failed: not support type [%s]", value.ConstraintType)
				}
			}
			return builder.String(), fmt.Errorf("tidb table [%s] constraint primary and unique key [%v] assert ConstraintPUKey failed, type: [%v]", c.MySQLTableINFO.TableName, pu, reflect.TypeOf(pu))
		}
	}
	return builder.String(), nil
}

func (c *Diff) CheckForeignKey() (string, error) {
	// TiDB 版本排除外键以及检查约束检查, skip
	return "", nil
}

func (c *Diff) CheckCheckKey() (string, error) {
	// TiDB 版本排除外键以及检查约束检查, skip
	return "", nil
}

func (c *Diff) CheckIndex() (string, error) {
	// 索引检查
	zap.L().Info("check table",
		zap.String("table indexes check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)),
		zap.String("oracle struct", c.OracleTableINFO.String(common.JSONIndex)),
		zap.String("tidb struct", c.MySQLTableINFO.String(common.JSONIndex)))

	var builder strings.Builder
	var createIndexSQL []string
	addDiffIndex, _, isOK := common.DiffStructArray(c.MySQLTableINFO.Indexes, c.OracleTableINFO.Indexes)
	if len(addDiffIndex) != 0 && !isOK {
		for _, idx := range addDiffIndex {
			value, ok := idx.(public.Index)
			if ok {
				if value.Uniqueness == "UNIQUE" && value.IndexType == "BTREE" {
					var equalArray []interface{}
					for _, oraIndexInfo := range c.OracleTableINFO.Indexes {
						if reflect.DeepEqual(value.IndexInfo, oraIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "BTREE" {
					var equalArray []interface{}
					for _, oraIndexInfo := range c.OracleTableINFO.Indexes {
						if reflect.DeepEqual(value.IndexInfo, oraIndexInfo.IndexInfo) {
							equalArray = append(equalArray, value.IndexInfo)
						}
					}
					if len(equalArray) == 0 {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);\n",
							value.IndexName, c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, value.IndexColumn))
					}
					continue
				}
				if value.Uniqueness == "NONUNIQUE" && value.IndexType == "FULLTEXT" {
					createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);\n",
						value.IndexName, c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName, value.IndexColumn))
					continue
				}
				return builder.String(), fmt.Errorf("tidb table [%s] diff failed, not support index: [%v]", c.MySQLTableINFO.TableName, value)
			}
			return builder.String(), fmt.Errorf("tidb table [%s] index [%v] assert Index failed, type: [%v]", c.MySQLTableINFO.TableName, idx, reflect.TypeOf(idx))
		}
	}

	if len(createIndexSQL) != 0 {
		builder.WriteString("/*\n")
		builder.WriteString(" tidb and oracle table indexes\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"TABLE", "INDEXES", "SUGGEST"})
		t.AppendRows([]table.Row{
			{c.OracleTableINFO.TableName, "TiDB And Oracle Different", "Create Table Index"},
		})
		builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

		builder.WriteString("*/\n")
		for _, indexSQL := range createIndexSQL {
			builder.WriteString(indexSQL)
		}
	}

	return builder.String(), nil
}

func (c *Diff) CheckPartitionTable() (string, error) {
	// 分区表检查
	var builder strings.Builder
	if c.MySQLTableINFO.IsPartition && c.OracleTableINFO.IsPartition {
		zap.L().Info("check table",
			zap.String("table partition check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)),
			zap.String("oracle struct", c.OracleTableINFO.String(common.JSONPartition)),
			zap.String("tidb struct", c.MySQLTableINFO.String(common.JSONPartition)))

		addDiffParts, _, isOK := common.DiffStructArray(c.MySQLTableINFO.Partitions, c.OracleTableINFO.Partitions)
		if len(addDiffParts) != 0 && !isOK {
			builder.WriteString("/*\n")
			builder.WriteString(" tidb and oracle table partitions\n")

			t := table.NewWriter()
			t.SetStyle(table.StyleLight)
			t.AppendHeader(table.Row{"TABLE", "PARTITIONS", "SUGGEST"})
			t.AppendRows([]table.Row{
				{c.OracleTableINFO.TableName, "TiDB And Oracle Different", "Manual Create Partition Table"},
			})
			builder.WriteString(fmt.Sprintf("%v\n", t.Render()))

			builder.WriteString("*/\n")
			builder.WriteString("-- mysql partition info exist, oracle partition isn't exist, please manual modify\n")

			for _, part := range addDiffParts {
				value, ok := part.(public.Partition)
				if ok {
					partJSON, err := json.Marshal(value)
					if err != nil {
						return builder.String(), err
					}
					builder.WriteString(fmt.Sprintf("# mysql partition info: %s, ", partJSON))
					continue
				}
				return builder.String(), fmt.Errorf("tidb table [%s] paritions [%v] assert Partition failed, type: [%v]", c.MySQLTableINFO.TableName, part, reflect.TypeOf(part))
			}
		}
	}
	return builder.String(), nil
}

func (c *Diff) CheckColumn() (string, error) {
	// 表字段检查
	// 注释格式化
	zap.L().Info("check table",
		zap.String("table column info check", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

	var (
		diffColumnMsgs []string
		tableRowArray  []table.Row
		builder        strings.Builder
	)

	for mysqlColName, mysqlColInfo := range c.MySQLTableINFO.Columns {
		oracleColInfo, ok := c.OracleTableINFO.Columns[mysqlColName]
		if ok {
			diffColumnMsg, tableRows, err := MySQLTableColumnMapRuleCheck(
				common.StringUPPER(c.MySQLTableINFO.SchemaName),
				common.StringUPPER(c.OracleTableINFO.SchemaName),
				common.StringUPPER(c.MySQLTableINFO.TableName),
				common.StringsBuilder("`", mysqlColName, "`"),
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
		// 如果源端字段不存在,则目标段字段忽略，功能与 MySQLTableColumnMapRuleCheck 函数相同，但对于源端存在目标端不存在的新增

	}

	if len(tableRowArray) != 0 && len(diffColumnMsgs) != 0 {
		zap.L().Info("check table",
			zap.String("table column info check, generate fixed sql", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)),
			zap.String("oracle struct", c.OracleTableINFO.String(common.JSONColumns)),
			zap.String("tidb struct", c.MySQLTableINFO.String(common.JSONColumns)))

		textTable := table.NewWriter()
		textTable.SetStyle(table.StyleLight)
		textTable.AppendHeader(table.Row{"TABLE", "COLUMN", "TIDB", "ORACLE", "SUGGEST"})
		textTable.AppendRows(tableRowArray)

		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" tidb table columns info is different from oracle\n"))
		builder.WriteString(fmt.Sprintf("%s\n", textTable.Render()))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("-- tidb table columns info is different from oracle, generate fixed sql\n"))
		for _, diffColMsg := range diffColumnMsgs {
			builder.WriteString(diffColMsg)
		}
		builder.WriteString("\n")
	}

	return builder.String(), nil
}

func (c *Diff) Writer(f *check.File) error {
	startTime := time.Now()
	zap.L().Info("check table start",
		zap.String("oracle table", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
		zap.String("tidb table", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)))

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
		if _, err := f.CWriteFile(builder.String()); err != nil {
			return err
		}
	}

	endTime := time.Now()
	zap.L().Info("check table finished",
		zap.String("oracle table", fmt.Sprintf("%s.%s", c.OracleTableINFO.SchemaName, c.OracleTableINFO.TableName)),
		zap.String("tidb table", fmt.Sprintf("%s.%s", c.MySQLTableINFO.SchemaName, c.MySQLTableINFO.TableName)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func (c *Diff) String() string {
	jsonStr, _ := json.Marshal(c)
	return string(jsonStr)
}
