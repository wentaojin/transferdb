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
	"context"
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
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
	MySQLDBType             string          `json:"mysqldb_type"`
	OracleDBVersion         string          `json:"oracle_db_version"`
	OracleExtendedMode      bool            `json:"oracle_extended_mode"`
	SourceSchemaName        string          `json:"source_schema_name"`
	TargetSchemaName        string          `json:"target_schema_name"`
	SourceTableName         string          `json:"source_table_name"`
	IsPartition             bool            `json:"is_partition"`
	TargetTableName         string          `json:"target_table_name"`
	SourceTableCharacterSet string          `json:"source_table_character_set"`
	SourceTableCollation    string          `json:"source_table_collation"`
	Overwrite               bool            `json:"overwrite"`
	Oracle                  *oracle.Oracle  `json:"-"`
	MySQL                   *mysql.MySQL    `json:"-"`
}

func PreCheckCompatibility(cfg *config.Config, mysql *mysql.MySQL, exporters []string, oracleDBVersion string, isExtended bool) ([]string, map[string][]map[string]string, map[string]string, map[string]string, error) {
	// MySQL CharacterSet And Collation 过滤检查
	tableCharSetMap := make(map[string]string)
	tableCollationMap := make(map[string]string)

	errCompatibility := make(map[string][]map[string]string)

	var (
		errCompINFO       []map[string]string
		reverseTaskTables []string
	)

	for _, t := range exporters {
		// 检查表级别字符集以及排序规则
		characterSet, collation, err := mysql.GetMySQLTableCharacterSetAndCollation(cfg.MySQLConfig.SchemaName, t)
		if err != nil {
			return []string{}, errCompatibility, tableCharSetMap, tableCollationMap, fmt.Errorf("get mysql table characterSet and collation falied: %v", err)
		}
		_, okTableCharacterSet := common.MySQLDBCharacterSetMap[common.StringUPPER(characterSet)]
		_, okTableCollation := common.MySQLDBCollationMap[strings.ToLower(collation)]

		if !okTableCharacterSet || !okTableCollation {
			errCompINFO = append(errCompINFO, map[string]string{
				"TableCharacterSet":  characterSet,
				"TableCollation":     collation,
				"ColumnCharacterSet": "",
				"ColumnCollation":    "",
				"ColumnType":         ""})
		}

		// 检查表字段级别字符集以及排序规则
		// 如果表级别字符集与字段级别字符集不一样，oracle 不支持
		// 如果 Oracle 版本
		columnsMap, err := mysql.GetMySQLTableColumn(cfg.MySQLConfig.SchemaName, t)
		if err != nil {
			return []string{}, errCompatibility, tableCharSetMap, tableCollationMap, fmt.Errorf("get mysql table column characterSet and collation falied: %v", err)
		}

		// 12.2 以下版本没有字段级别 collation，使用 oracledb 实例级别 collation
		// 检查表以及字段级别 collation 是否一致等于 utf8mb4_bin / utf8_bin，不一致则输出
		if common.VersionOrdinal(oracleDBVersion) < common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
			for _, rowCol := range columnsMap {
				// 检查字段级别排序规则
				_, ok := common.MySQLDBCollationMap[strings.ToLower(rowCol["COLLATION_NAME"])]

				if common.IsContainString(common.OracleIsNotSupportDataType, rowCol["DATA_TYPE"]) ||
					(!strings.EqualFold(rowCol["CHARACTER_SET_NAME"], "UNKNOWN") && !strings.EqualFold(rowCol["CHARACTER_SET_NAME"], characterSet)) ||
					(!ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN")) ||
					(ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "utf8mb4_bin")) ||
					(ok && !strings.EqualFold(collation, "utf8mb4_bin")) ||
					(ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "utf8_bin")) ||
					(ok && !strings.EqualFold(collation, "utf8_bin")) {
					errCompINFO = append(errCompINFO, map[string]string{
						"TableCharacterSet":  characterSet,
						"TableCollation":     collation,
						"ColumnCharacterSet": fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"]),
						"ColumnCollation":    fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"]),
						"ColumnType":         fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"]),
					})
				}
			}
		}

		if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
			for _, rowCol := range columnsMap {
				// 检查字段级别排序规则
				_, ok := common.MySQLDBCollationMap[strings.ToLower(rowCol["COLLATION_NAME"])]

				if common.IsContainString(common.OracleIsNotSupportDataType, rowCol["DATA_TYPE"]) ||
					(!strings.EqualFold(rowCol["CHARACTER_SET_NAME"], "UNKNOWN") && !strings.EqualFold(rowCol["CHARACTER_SET_NAME"], characterSet)) ||
					(!ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN")) ||
					(!isExtended && !strings.EqualFold(rowCol["COLLATION_NAME"], collation)) ||
					(!isExtended && !strings.EqualFold(rowCol["CHARACTER_SET_NAME"], characterSet)) {
					errCompINFO = append(errCompINFO, map[string]string{
						"TableCharacterSet":  characterSet,
						"TableCollation":     collation,
						"ColumnCharacterSet": fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"]),
						"ColumnCollation":    fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"]),
						"ColumnType":         fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"]),
					})
				}
			}
		}

		if len(errCompINFO) > 0 {
			errCompatibility[common.StringUPPER(t)] = errCompINFO
		}

		_, okErrCharSet := errCompatibility[common.StringUPPER(t)]
		// 筛选过滤不兼容表
		// Skip 当前循环，继续
		if okErrCharSet {
			continue
		}
		tableCharSetMap[common.StringUPPER(t)] = characterSet
		tableCollationMap[common.StringUPPER(t)] = collation
		reverseTaskTables = append(reverseTaskTables, common.StringUPPER(t))
	}
	return reverseTaskTables, errCompatibility, tableCharSetMap, tableCollationMap, nil
}

func GenReverseTableTask(cfg *config.Config, mysql *mysql.MySQL, oracle *oracle.Oracle, tableNameRule map[string]string, exporters []string, oracleDBVersion string, isExtended bool, tableCharSetMap map[string]string, tableCollationMap map[string]string) ([]*Table, error) {
	var (
		tables []*Table
	)

	sourceSchema := common.StringUPPER(cfg.MySQLConfig.SchemaName)
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

	partitionTables, err := mysql.GetMySQLPartitionTable(cfg.MySQLConfig.SchemaName)
	if err != nil {
		return tables, err
	}

	g1 := &errgroup.Group{}
	tableChan := make(chan *Table, common.BufferSize)

	g1.Go(func() error {
		g2 := &errgroup.Group{}
		g2.SetLimit(cfg.AppConfig.Threads)

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
					MySQLDBType:             cfg.MySQLConfig.DBType,
					OracleDBVersion:         oracleDBVersion,
					OracleExtendedMode:      isExtended,
					SourceSchemaName:        common.StringUPPER(sourceSchema),
					SourceTableName:         common.StringUPPER(ts),
					TargetSchemaName:        common.StringUPPER(cfg.OracleConfig.SchemaName),
					TargetTableName:         targetTableName,
					IsPartition:             common.IsContainString(partitionTables, common.StringUPPER(ts)),
					SourceTableCharacterSet: tableCharSetMap[ts],
					SourceTableCollation:    tableCollationMap[ts],
					Overwrite:               cfg.MySQLConfig.Overwrite,
					MySQL:                   mysql,
					Oracle:                  oracle,
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
	if strings.EqualFold(t.MySQLDBType, common.TaskDBTiDB) {
		return nil, nil
	}
	return t.MySQL.GetMySQLTableCheckKey(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableUniqueIndex() ([]map[string]string, error) {
	// MySQL Unique Index = Unique Constraint
	return nil, nil
}

func (t *Table) GetTableNormalIndex() ([]map[string]string, error) {
	return t.MySQL.GetMySQLTableIndex(t.SourceSchemaName, t.SourceTableName)
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

func (t *Table) String() string {
	jsonStr, _ := json.Marshal(t)
	return string(jsonStr)
}

func GenCreateSchema(f *reverse.File, sourceSchema, targetSchema string) error {
	startTime := time.Now()
	var (
		sqlRev strings.Builder
	)

	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(" mysql schema reverse oracle database\n")
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"#", "MySQL", "ORACLE", "SUGGEST"})
	t.AppendRows([]table.Row{
		{"Schema", sourceSchema, targetSchema, "Create Schema"},
	})
	sqlRev.WriteString(t.Render() + "\n")
	sqlRev.WriteString("*/\n")

	sqlRev.WriteString(fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s;\n\n", common.StringUPPER(targetSchema), common.StringUPPER(targetSchema)))

	if _, err := f.RWriteString(sqlRev.String()); err != nil {
		return err
	}
	endTime := time.Now()
	zap.L().Info("output mysql to oracle schema create sql",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func GenCompatibilityTable(f *reverse.File, sourceSchema string, errCompatibility map[string][]map[string]string, viewTables []string) error {
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

			if _, err := f.CWriteString(sqlComp.String()); err != nil {
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

		if _, err := f.CWriteString(sqlComp.String()); err != nil {
			return err
		}
	}

	endTime := time.Now()
	zap.L().Info("output mysql to oracle compatibility tips",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
