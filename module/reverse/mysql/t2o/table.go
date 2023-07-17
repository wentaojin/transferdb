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
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/reverse"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"
)

type Table struct {
	Ctx                     context.Context `json:"-"`
	OracleDBVersion         string          `json:"oracle_db_version"`
	OracleExtendedMode      bool            `json:"oracle_extended_mode"`
	SourceSchemaName        string          `json:"source_schema_name"`
	TargetSchemaName        string          `json:"target_schema_name"`
	SourceTableName         string          `json:"source_table_name"`
	TargetTableName         string          `json:"target_table_name"`
	IsPartition             bool            `json:"is_partition"`
	SourceTableCharacterSet string          `json:"source_table_character_set"`
	SourceTableCollation    string          `json:"source_table_collation"`
	SourceDBCharset         string          `json:"sourcedb_charset"`
	TargetDBCharset         string          `json:"targetdb_charset"`

	TableColumnDatatypeRule   map[string]string `json:"table_column_datatype_rule"`
	TableColumnDefaultValRule map[string]string `json:"table_column_default_val_rule"`
	Overwrite                 bool              `json:"overwrite"`
	Oracle                    *oracle.Oracle    `json:"-"`
	MySQL                     *mysql.MySQL      `json:"-"`
	MetaDB                    *meta.Meta        `json:"-"`
}

func PreCheckCompatibility(cfg *config.Config, mysql *mysql.MySQL, exporters []string, oracleDBVersion, oracleDBCharset string, isExtended bool) ([]string, map[string][]map[string]string, map[string][]map[string]string, map[string]string, map[string]string, error) {
	// MySQL Charset And Collation 过滤检查
	tableCharSetMap := make(map[string]string)
	tableCollationMap := make(map[string]string)

	errCompatibilityTable := make(map[string][]map[string]string)
	errCompatibilityColumn := make(map[string][]map[string]string)

	var (
		reverseTaskTables []string
	)

	for _, t := range exporters {
		var (
			errTableCompINFO  []map[string]string
			errColumnCompINFO []map[string]string
		)

		// 检查表级别字符集以及排序规则
		sourceTableCharacterSet, sourceTableCollation, err := mysql.GetMySQLTableCharacterSetAndCollation(cfg.SchemaConfig.SourceSchema, t)
		if err != nil {
			return []string{}, errCompatibilityTable, errCompatibilityColumn, tableCharSetMap, tableCollationMap, fmt.Errorf("get mysql table characterSet and collation falied: %v", err)
		}

		sourceSchemaTableName := fmt.Sprintf("%s.%s", cfg.SchemaConfig.SourceSchema, t)

		targetTableCharset, okTableCharacterSet := common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(sourceTableCharacterSet)]
		targetTableCollation, okTableCollation := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(sourceTableCollation)][common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(sourceTableCharacterSet)]]

		// 判断 oracle 是否支持表字符集以及排序规则以及是否跟现有 oracle 环境字符集相同
		if !okTableCharacterSet || !okTableCollation || !strings.EqualFold(oracleDBCharset, targetTableCharset) {
			errTableCompINFO = append(errTableCompINFO, map[string]string{
				"SourceTable":          sourceSchemaTableName,
				"SourceTableCharset":   sourceTableCharacterSet,
				"SourceTableCollation": sourceTableCollation,
				"TargetTableCharset":   targetTableCharset,
				"TargetTableCollation": targetTableCollation,
				"CurrentOracleCharset": oracleDBCharset,
				"Suggest":              "Manual Process Table"})
		}

		if len(errTableCompINFO) > 0 {
			errCompatibilityTable[sourceSchemaTableName] = errTableCompINFO
		}
		_, okErrTable := errCompatibilityTable[sourceSchemaTableName]

		// 筛选过滤不兼容表
		// Skip 当前循环，继续下一张表
		if okErrTable {
			continue
		}

		// 检查表字段级别字符集以及排序规则
		// 如果表级别字符集与字段级别字符集不一样，oracle 不支持
		columnsMap, err := mysql.GetMySQLTableColumn(cfg.SchemaConfig.SourceSchema, t)
		if err != nil {
			return []string{}, errCompatibilityTable, errCompatibilityColumn, tableCharSetMap, tableCollationMap, fmt.Errorf("get mysql table column characterSet and collation falied: %v", err)
		}

		// 12.2 以下版本没有字段级别 collation，使用 oracledb 实例级别 collation
		// 1、检查表以及字段级别的 charset 是否一致,不一致则输出 或者表以及字段级别 charset 一致但 oracle 不支持 或者 oracle 不支持
		// 2、检查表以及字段级别 collation 是否一致且是 %_bin 带 bin 的排序规则,不一致则输出 或者表以及字段级别 collation 一致但 oracle 不支持 或者 oracle 不支持
		if common.VersionOrdinal(oracleDBVersion) < common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
			for _, rowCol := range columnsMap {

				tmpMap := make(map[string]string)

				sourceColumnCharset := strings.ToUpper(rowCol["CHARACTER_SET_NAME"])
				sourceColumnCollation := strings.ToUpper(rowCol["COLLATION_NAME"])

				// 检查字段级别排序规则
				_, okColumnCharset := common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["CHARACTER_SET_NAME"])]
				_, okColumnCollation := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["COLLATION_NAME"])][common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["CHARACTER_SET_NAME"])]]

				if common.IsContainString(common.OracleIsNotSupportDataType, rowCol["DATA_TYPE"]) {
					errColumnCompINFO = append(errColumnCompINFO, map[string]string{
						"SourceTable":           sourceSchemaTableName,
						"SourceTableCharset":    sourceTableCharacterSet,
						"SourceTableCollation":  sourceTableCollation,
						"SourceColumnCharset":   fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"]),
						"SourceColumnCollation": fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"]),
						"SourceColumnType":      fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"]),
						"PreCheckType":          "DATA_TYPE",
						"Suggest":               "Manual Process Table"})
				} else {

					// 排除字符集是 UNKNOWN 的整型字段
					if !strings.EqualFold(strings.ToUpper(rowCol["CHARACTER_SET_NAME"]), "UNKNOWN") || !strings.EqualFold(strings.ToUpper(rowCol["COLLATION_NAME"]), "UNKNOWN") {
						// 检查表以及字段级别的 charset 是否一致,不一致则输出 或者表以及字段级别 charset 一致但 oracle 不支持 或者 oracle 不支持

						if (!strings.EqualFold(sourceTableCharacterSet, sourceColumnCharset)) || (strings.EqualFold(sourceTableCharacterSet, sourceColumnCharset) && !okColumnCharset) || !okColumnCharset {
							tmpMap["SourceTable"] = sourceSchemaTableName
							tmpMap["SourceTableCharset"] = sourceTableCharacterSet
							tmpMap["SourceTableCollation"] = sourceTableCollation
							tmpMap["SourceColumnCharset"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"])
							tmpMap["SourceColumnCollation"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
							tmpMap["SourceColumnType"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"])
							tmpMap["PreCheckType"] = "CHARSET OR COLLATION"
							tmpMap["Suggest"] = "Manual Process Table"
						}
						// 检查表以及字段级别 collation 是否一致且是 %_bin 带 bin 的排序规则,不一致则输出 或者表以及字段级别 collation 一致但 oracle 不支持 或者 oracle 不支持
						if (!strings.EqualFold(sourceTableCollation, sourceColumnCollation)) ||
							(strings.EqualFold(sourceTableCollation, sourceColumnCollation) && !okColumnCollation) || (strings.EqualFold(sourceTableCollation, sourceColumnCollation) && okColumnCollation && !strings.Contains(strings.ToUpper(sourceTableCollation), "BIN")) || !okColumnCollation {
							tmpMap["SourceTable"] = sourceSchemaTableName
							tmpMap["SourceTableCharset"] = sourceTableCharacterSet
							tmpMap["SourceTableCollation"] = sourceTableCollation
							tmpMap["SourceColumnCharset"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"])
							tmpMap["SourceColumnCollation"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
							tmpMap["SourceColumnType"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"])
							tmpMap["PreCheckType"] = "CHARSET OR COLLATION"
							tmpMap["Suggest"] = "Manual Process Table"
						}
					}

					if len(tmpMap) > 0 {
						errColumnCompINFO = append(errColumnCompINFO, tmpMap)
					}
				}
			}
		}

		// 12.2 以上版本
		// 参数 isExtended false, 没有字段级别 collation，使用 oracledb 实例级别 collation
		// 1、检查表以及字段级别的 charset 是否一致,不一致则输出 或者表以及字段级别 charset 一致但 oracle 不支持 或者 oracle 不支持
		// 2、检查表以及字段级别 collation 是否一致且是 %_bin 带 bin 的排序规则,不一致则输出 或者表以及字段级别 collation 一致但 oracle 不支持 或者 oracle 不支持
		// 参数 isExtended true, 有字段级别 collation
		// 1、检查表以及字段级别 charset 是否一致 或者 oracle 是否支持
		// 2、检查表以及字段级别 collation 是否一致 或者 oracle 是否支持
		if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
			for _, rowCol := range columnsMap {
				tmpMap := make(map[string]string)

				sourceColumnCharset := strings.ToUpper(rowCol["CHARACTER_SET_NAME"])
				sourceColumnCollation := strings.ToUpper(rowCol["COLLATION_NAME"])

				// 检查字段级别排序规则
				_, okColumnCharset := common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["CHARACTER_SET_NAME"])]
				_, okColumnCollation := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["COLLATION_NAME"])][common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["CHARACTER_SET_NAME"])]]

				if common.IsContainString(common.OracleIsNotSupportDataType, rowCol["DATA_TYPE"]) {
					errColumnCompINFO = append(errColumnCompINFO, map[string]string{
						"SourceTable":           sourceSchemaTableName,
						"SourceTableCharset":    sourceTableCharacterSet,
						"SourceTableCollation":  sourceTableCollation,
						"SourceColumnCharset":   fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"]),
						"SourceColumnCollation": fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"]),
						"SourceColumnType":      fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"]),
						"PreCheckType":          "DATA_TYPE",
						"Suggest":               "Manual Process Table"})
				} else {
					if isExtended {
						// 排除字符集是 UNKNOWN 的整型字段
						if !strings.EqualFold(strings.ToUpper(rowCol["CHARACTER_SET_NAME"]), "UNKNOWN") || !strings.EqualFold(strings.ToUpper(rowCol["COLLATION_NAME"]), "UNKNOWN") {
							// 检查表以及字段级别 charset 是否一致 或者 oracle 是否支持
							if (!strings.EqualFold(sourceTableCharacterSet, sourceColumnCharset)) || (strings.EqualFold(sourceTableCharacterSet, sourceColumnCharset) && !okColumnCharset) {
								tmpMap["SourceTable"] = sourceSchemaTableName
								tmpMap["SourceTableCharset"] = sourceTableCharacterSet
								tmpMap["SourceTableCollation"] = sourceTableCollation
								tmpMap["SourceColumnCharset"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"])
								tmpMap["SourceColumnCollation"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
								tmpMap["SourceColumnType"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"])
								tmpMap["PreCheckType"] = "CHARSET OR COLLATION"
								tmpMap["Suggest"] = "Manual Process Table"
							}
							// 检查表以及字段级别 collation 是否一致 或者 oracle 是否支持
							if (!strings.EqualFold(sourceTableCollation, sourceColumnCollation)) || (strings.EqualFold(sourceTableCollation, sourceColumnCollation) && !okColumnCollation) {
								tmpMap["SourceTable"] = sourceSchemaTableName
								tmpMap["SourceTableCharset"] = sourceTableCharacterSet
								tmpMap["SourceTableCollation"] = sourceTableCollation
								tmpMap["SourceColumnCharset"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"])
								tmpMap["SourceColumnCollation"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
								tmpMap["SourceColumnType"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"])
								tmpMap["PreCheckType"] = "CHARSET OR COLLATION"
								tmpMap["Suggest"] = "Manual Process Table"
							}
						}
					} else {
						// 排除字符集是 UNKNOWN 的整型字段
						if !strings.EqualFold(strings.ToUpper(rowCol["CHARACTER_SET_NAME"]), "UNKNOWN") || !strings.EqualFold(strings.ToUpper(rowCol["COLLATION_NAME"]), "UNKNOWN") {
							// 检查表以及字段级别的 charset 是否一致,不一致则输出 或者表以及字段级别 charset 一致但 oracle 不支持 或者 oracle 不支持
							if (!strings.EqualFold(sourceTableCharacterSet, sourceColumnCharset)) || (strings.EqualFold(sourceTableCharacterSet, sourceColumnCharset) && !okColumnCharset) || !okColumnCharset {
								tmpMap["SourceTable"] = sourceSchemaTableName
								tmpMap["SourceTableCharset"] = sourceTableCharacterSet
								tmpMap["SourceTableCollation"] = sourceTableCollation
								tmpMap["SourceColumnCharset"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"])
								tmpMap["SourceColumnCollation"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
								tmpMap["SourceColumnType"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"])
								tmpMap["PreCheckType"] = "CHARSET OR COLLATION"
								tmpMap["Suggest"] = "Manual Process Table"
							}
							// 检查表以及字段级别 collation 是否一致且是 %_bin 带 bin 的排序规则,不一致则输出 或者表以及字段级别 collation 一致但 oracle 不支持 或者 oracle 不支持
							if (!strings.EqualFold(sourceTableCollation, sourceColumnCollation)) ||
								(strings.EqualFold(sourceTableCollation, sourceColumnCollation) && !okColumnCollation) || (strings.EqualFold(sourceTableCollation, sourceColumnCollation) && okColumnCollation && !strings.Contains(strings.ToUpper(sourceTableCollation), "BIN")) || !okColumnCollation {
								tmpMap["SourceTable"] = sourceSchemaTableName
								tmpMap["SourceTableCharset"] = sourceTableCharacterSet
								tmpMap["SourceTableCollation"] = sourceTableCollation
								tmpMap["SourceColumnCharset"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"])
								tmpMap["SourceColumnCollation"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
								tmpMap["SourceColumnType"] = fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"])
								tmpMap["PreCheckType"] = "CHARSET OR COLLATION"
								tmpMap["Suggest"] = "Manual Process Table"
							}
						}
					}

					if len(tmpMap) > 0 {
						errColumnCompINFO = append(errColumnCompINFO, tmpMap)
					}
				}
			}
		}

		if len(errColumnCompINFO) > 0 {
			errCompatibilityColumn[sourceSchemaTableName] = errColumnCompINFO
		}

		_, okErrColumn := errCompatibilityColumn[sourceSchemaTableName]

		// 筛选过滤不兼容表
		// Skip 当前循环，继续下一张表
		if okErrColumn || okErrTable {
			continue
		}

		tableCharSetMap[common.StringUPPER(t)] = sourceTableCharacterSet
		tableCollationMap[common.StringUPPER(t)] = sourceTableCollation
		reverseTaskTables = append(reverseTaskTables, common.StringUPPER(t))
	}
	return reverseTaskTables, errCompatibilityTable, errCompatibilityColumn, tableCharSetMap, tableCollationMap, nil
}

func GenReverseTableTask(r *Reverse, tableNameRule map[string]string, tableColumnRule, tableDefaultRule map[string]map[string]string, exporters []string, oracleDBVersion string, isExtended bool, tableCharSetMap map[string]string, tableCollationMap map[string]string) ([]*Table, error) {
	var (
		tables []*Table
	)

	sourceSchema := common.StringUPPER(r.cfg.SchemaConfig.SourceSchema)
	beginTime := time.Now()
	defer func() {
		endTime := time.Now()
		zap.L().Info("gen oracle table list finished",
			zap.String("schema", sourceSchema),
			zap.Int("table totals", len(exporters)),
			zap.Int("table gens", len(tables)),
			zap.String("cost", endTime.Sub(beginTime).String()))
	}()

	startTime := time.Now()

	partitionTables, err := r.mysql.GetMySQLPartitionTable(r.cfg.SchemaConfig.SourceSchema)
	if err != nil {
		return tables, err
	}

	g1 := &errgroup.Group{}
	tableChan := make(chan *Table, common.ChannelBufferSize)

	g1.Go(func() error {
		g2 := &errgroup.Group{}
		g2.SetLimit(r.cfg.ReverseConfig.ReverseThreads)

		for _, t := range exporters {
			ts := t
			g2.Go(func() error {
				var targetTableName string
				if val, ok := tableNameRule[common.StringUPPER(ts)]; ok {
					targetTableName = val
				} else {
					targetTableName = common.StringUPPER(ts)
				}

				tbl := &Table{
					OracleDBVersion:           oracleDBVersion,
					OracleExtendedMode:        isExtended,
					SourceSchemaName:          common.StringUPPER(sourceSchema),
					SourceTableName:           common.StringUPPER(ts),
					TargetSchemaName:          common.StringUPPER(r.cfg.SchemaConfig.TargetSchema),
					TargetTableName:           targetTableName,
					IsPartition:               common.IsContainString(partitionTables, common.StringUPPER(ts)),
					SourceTableCharacterSet:   tableCharSetMap[ts],
					SourceTableCollation:      tableCollationMap[ts],
					SourceDBCharset:           r.cfg.MySQLConfig.Charset,
					TargetDBCharset:           r.cfg.OracleConfig.Charset,
					TableColumnDatatypeRule:   tableColumnRule[common.StringUPPER(ts)],
					TableColumnDefaultValRule: tableDefaultRule[common.StringUPPER(ts)],
					Overwrite:                 r.cfg.MySQLConfig.Overwrite,
					MySQL:                     r.mysql,
					Oracle:                    r.oracle,
					MetaDB:                    r.metaDB,
				}
				tableChan <- tbl
				return nil
			})
		}

		if err = g2.Wait(); err != nil {
			return err
		}
		close(tableChan)
		return nil
	})

	// 数据通道接收
	for c := range tableChan {
		tables = append(tables, c)
	}

	err = g1.Wait()
	if err != nil {
		return nil, err
	}

	endTime := time.Now()
	zap.L().Info("gen mysql slice table finished",
		zap.String("schema", sourceSchema),
		zap.Int("table totals", len(exporters)),
		zap.Int("table gens", len(tables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return tables, nil
}

func (t *Table) GetTablePrimaryKey() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTablePrimaryKey(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableUniqueKey() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTableUniqueKey(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableForeignKey() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTableForeignKey(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableCheckKey() ([]map[string]string, error) {
	// T2O Skip
	return nil, nil
}

func (t *Table) GetTableUniqueIndex() ([]map[string]string, error) {
	// MySQL Unique Index = Unique Constraint
	return nil, nil
}

func (t *Table) GetTableNormalIndex() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTableNormalIndex(t.SourceSchemaName, t.SourceTableName, common.DatabaseTypeTiDB)
}

func (t *Table) GetTableComment() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTableComment(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableColumnMeta() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTableColumn(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableColumnComment() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTableColumnComment(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableInfo() (interface{}, error) {
	primaryKey, err := t.GetTablePrimaryKey()
	if err != nil {
		return nil, err
	}
	uniqueKey, err := t.GetTableUniqueKey()
	if err != nil {
		return nil, err
	}
	foreignKey, err := t.GetTableForeignKey()
	if err != nil {
		return nil, err
	}
	checkKey, err := t.GetTableCheckKey()
	if err != nil {
		return nil, err
	}
	uniqueIndex, err := t.GetTableUniqueIndex()
	if err != nil {
		return nil, err
	}
	normalIndex, err := t.GetTableNormalIndex()
	if err != nil {
		return nil, err
	}
	tableComment, err := t.GetTableComment()
	if err != nil {
		return nil, err
	}
	columnMeta, err := t.GetTableColumnMeta()
	if err != nil {
		return nil, err
	}
	// M2O -> mysql/tidb need, because oracle comment sql special
	// O2M -> it is not need
	columnComment, err := t.GetTableColumnComment()
	if err != nil {
		return nil, err
	}
	tablePartitionDetail, err := t.GetTablePartitionDetail()
	if err != nil {
		return nil, err
	}

	ddl, err := t.GetTableOriginDDL()
	if err != nil {
		return nil, err
	}

	return &Info{
		SourceTableDDL:       ddl,
		PrimaryKeyINFO:       primaryKey,
		UniqueKeyINFO:        uniqueKey,
		ForeignKeyINFO:       foreignKey,
		CheckKeyINFO:         checkKey,
		UniqueIndexINFO:      uniqueIndex,
		NormalIndexINFO:      normalIndex,
		TableCommentINFO:     tableComment,
		TableColumnINFO:      columnMeta,
		ColumnCommentINFO:    columnComment,
		TablePartitionDetail: tablePartitionDetail,
	}, nil
}

func (t *Table) GetTableOriginDDL() (string, error) {
	ddl, err := t.MySQL.GetMySQLTableOriginDDL(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return ddl, err
	}
	return ddl, nil
}

func (t *Table) GetTablePartitionDetail() (string, error) {
	if t.IsPartition {
		partitionDetail, err := t.MySQL.GetMySQLPartitionTableDetailINFO(t.SourceSchemaName, t.SourceTableName)
		if err != nil {
			return partitionDetail, err
		}
		return partitionDetail, nil
	}
	return "", nil
}

func (t *Table) String() string {
	jsonStr, _ := json.Marshal(t)
	return string(jsonStr)
}

func GenCreateSchema(w *reverse.Write, sourceSchema, targetSchema string, directWrite bool) error {
	startTime := time.Now()
	var (
		sqlRev strings.Builder
	)

	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(" tidb schema reverse oracle database\n")
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"#", "TIDB", "ORACLE", "SUGGEST"})
	t.AppendRows([]table.Row{
		{"Schema", sourceSchema, targetSchema, "Create Schema"},
	})
	sqlRev.WriteString(t.Render() + "\n")
	sqlRev.WriteString("*/\n")

	sqlRev.WriteString(fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s;\n\n", common.StringUPPER(targetSchema), common.StringUPPER(targetSchema)))

	if directWrite {
		if err := w.RWriteDB(sqlRev.String()); err != nil {
			return err
		}
	} else {
		if _, err := w.RWriteFile(sqlRev.String()); err != nil {
			return err
		}
	}
	endTime := time.Now()
	zap.L().Info("output tidb to oracle schema create sql",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func GenCompatibilityTable(w *reverse.Write, sourceSchema string, errCompatibilityTable, errCompatibilityColumn map[string][]map[string]string, viewTables []string) error {
	startTime := time.Now()
	// 兼容提示
	if len(errCompatibilityTable) > 0 {
		// Table 级别

		var sqlComp strings.Builder

		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" tidb table maybe oracle has compatibility, please manual process\n")
		sqlComp.WriteString(" - tidb table charset and collation current isn't support\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)

		t.AppendHeader(table.Row{"Source Table", "Source Table Charset", "Source Table Collation", "Target Table Charset", "Target Table Collation", "Current Oracle Charset", "Suggest"})

		for _, info := range errCompatibilityTable {
			for _, compINFO := range info {
				t.AppendRows([]table.Row{
					{compINFO["SourceTable"], compINFO["SourceTableCharset"], compINFO["SourceTableCollation"],
						compINFO["TargetTableCharset"], compINFO["TargetTableCollation"], compINFO["CurrentOracleCharset"], compINFO["Suggest"]},
				})
			}
		}

		sqlComp.WriteString(t.Render() + "\n")
		sqlComp.WriteString("*/\n")

		if _, err := w.CWriteFile(sqlComp.String()); err != nil {
			return err
		}
	}

	if len(errCompatibilityColumn) > 0 {
		// COLUMN 级别
		var sqlComp strings.Builder

		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" tidb table maybe oracle has compatibility, please manual process\n")
		sqlComp.WriteString(" - tidb table charset and column charset isn't the same, or column charset currently isn't support\n")
		sqlComp.WriteString(" - tidb table collation and column collation isn't the same, or column collation currently isn't support\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)

		t.AppendHeader(table.Row{"Source Table", "Source Table Charset", "Source Table Collation", "Source Column Charset", "Source Column Collation", "Source Column Type", "Pre Check Type", "SUGGEST"})

		for _, info := range errCompatibilityColumn {
			for _, compINFO := range info {
				t.AppendRows([]table.Row{
					{compINFO["SourceTable"], compINFO["SourceTableCharset"], compINFO["SourceTableCollation"],
						compINFO["SourceColumnCharset"], compINFO["SourceColumnCollation"], compINFO["SourceColumnType"], compINFO["PreCheckType"], compINFO["Suggest"]},
				})
			}
		}
		sqlComp.WriteString(t.Render() + "\n")
		sqlComp.WriteString("*/\n")

		if _, err := w.CWriteFile(sqlComp.String()); err != nil {
			return err
		}
	}

	if len(viewTables) > 0 {
		var sqlComp strings.Builder

		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" tidb table maybe oracle has compatibility, please manual process\n")
		sqlComp.WriteString(" - tidb view current isn't support\n")

		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"SCHEMA", "TABLE NAME", "TABLE TYPE", "SUGGEST"})

		for _, viewName := range viewTables {
			t.AppendRows([]table.Row{{sourceSchema, viewName, "VIEW", "Manual Process Table"}})
		}
		sqlComp.WriteString(t.Render() + "\n")
		sqlComp.WriteString("*/\n")

		if _, err := w.CWriteFile(sqlComp.String()); err != nil {
			return err
		}
	}

	endTime := time.Now()
	zap.L().Info("output tidb to oracle compatibility tips",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
