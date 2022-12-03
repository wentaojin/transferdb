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
	"github.com/valyala/fastjson"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/module/query/mysql"
	"github.com/wentaojin/transferdb/module/query/oracle"
	"github.com/wentaojin/transferdb/module/reverse"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"
)

type Table struct {
	Ctx                   context.Context `json:"-"`
	SourceSchemaName      string          `json:"source_schema_name"`
	TargetSchemaName      string          `json:"target_schema_name"`
	SourceTableName       string          `json:"source_table_name"`
	TargetDBType          string          `json:"target_db_type"`
	TargetDBVersion       string          `json:"target_db_version"`
	TargetTableName       string          `json:"target_table_name"`
	TargetTableOption     string          `json:"target_table_option"`
	OracleCollation       bool            `json:"oracle_collation"`
	SourceSchemaCollation string          `json:"source_schema_collation"` // 可为空
	SourceTableCollation  string          `json:"source_table_collation"`  // 可为空
	SourceDBNLSSort       string          `json:"sourcedb_nlssort"`
	SourceDBNLSComp       string          `json:"sourcedb_nlscomp"`
	SourceTableType       string          `json:"source_table_type"`
	Overwrite             bool            `json:"overwrite"`
	Oracle                *oracle.Oracle  `json:"-"`
	MySQL                 *mysql.MySQL    `json:"-"`
}

func GenReverseTableTask(ctx context.Context, cfg *config.Config, mysql *mysql.MySQL, oracle *oracle.Oracle, exporters []string, nlsSort, nlsComp string) ([]*Table, error) {
	var tables []*Table

	beginTime := time.Now()
	defer func() {
		endTime := time.Now()
		zap.L().Info("gen oracle table list finished",
			zap.String("schema", cfg.OracleConfig.SchemaName),
			zap.Int("table totals", len(exporters)),
			zap.Int("table gens", len(tables)),
			zap.String("cost", endTime.Sub(beginTime).String()))
	}()

	// 获取 oracle 环境信息
	startTime := time.Now()
	characterSet, err := oracle.GetOracleDBCharacterSet()
	if err != nil {
		return tables, err
	}
	if _, ok := common.OracleDBCharacterSetMap[strings.Split(characterSet, ".")[1]]; !ok {
		return tables, fmt.Errorf("oracle db character set [%v] isn't support", characterSet)
	}

	// oracle 版本是否可指定表、字段 collation
	// oracle db nls_sort/nls_comp 值需要相等，USING_NLS_COMP 值取 nls_comp
	oraDBVersion, err := oracle.GetOracleDBVersion()
	if err != nil {
		return tables, err
	}

	oraCollation := false
	if common.VersionOrdinal(oraDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oraCollation = true
	}

	endTime := time.Now()
	zap.L().Info("get oracle db character and version finished",
		zap.String("schema", cfg.OracleConfig.SchemaName),
		zap.String("db version", oraDBVersion),
		zap.String("db character", characterSet),
		zap.Int("table totals", len(exporters)),
		zap.Bool("table collation", oraCollation),
		zap.String("cost", endTime.Sub(startTime).String()))

	var (
		tblCollation    map[string]string
		schemaCollation string
	)

	if oraCollation {
		startTime = time.Now()
		schemaCollation, err = oracle.GetOracleSchemaCollation(common.StringUPPER(cfg.OracleConfig.SchemaName))
		if err != nil {
			return tables, err
		}
		tblCollation, err = oracle.GetOracleSchemaTableCollation(common.StringUPPER(cfg.OracleConfig.SchemaName), schemaCollation)
		if err != nil {
			return tables, err
		}
		endTime = time.Now()
		zap.L().Info("get oracle schema and table collation finished",
			zap.String("schema", cfg.OracleConfig.SchemaName),
			zap.String("db version", oraDBVersion),
			zap.String("db character", characterSet),
			zap.Int("table totals", len(exporters)),
			zap.Bool("table collation", oraCollation),
			zap.String("cost", endTime.Sub(startTime).String()))
	}

	startTime = time.Now()
	tablesMap, err := oracle.GetOracleSchemaTableType(common.StringUPPER(cfg.OracleConfig.SchemaName))
	if err != nil {
		return tables, err
	}
	endTime = time.Now()
	zap.L().Info("get oracle table type finished",
		zap.String("schema", cfg.OracleConfig.SchemaName),
		zap.String("db version", oraDBVersion),
		zap.String("db character", characterSet),
		zap.Int("table totals", len(exporters)),
		zap.Bool("table collation", oraCollation),
		zap.String("cost", endTime.Sub(startTime).String()))

	// 获取 MySQL 版本
	mysqlVersion, err := mysql.GetMySQLDBVersion()
	if err != nil {
		return nil, err
	}

	var dbVersion string

	if strings.ToUpper(strings.ToUpper(cfg.MySQLConfig.DBType)) == common.TiDBTargetDBType {
		dbVersion = mysqlVersion
	} else {
		if strings.Contains(mysqlVersion, common.MySQLVersionDelimiter) {
			dbVersion = strings.Split(mysqlVersion, common.MySQLVersionDelimiter)[0]
		} else {
			dbVersion = mysqlVersion
		}
	}

	startTime = time.Now()
	g := &errgroup.Group{}
	g.SetLimit(cfg.AppConfig.Threads)
	tableChan := make(chan *Table, common.BufferSize)

	go func() {
		for c := range tableChan {
			tables = append(tables, c)
		}
	}()

	for _, exporter := range exporters {
		table := exporter
		g.Go(func() error {
			// 库名、表名规则
			tbl := &Table{
				Ctx:               ctx,
				SourceSchemaName:  strings.ToUpper(cfg.OracleConfig.SchemaName),
				TargetSchemaName:  strings.ToUpper(cfg.MySQLConfig.SchemaName),
				SourceTableName:   strings.ToUpper(table),
				TargetDBType:      strings.ToUpper(cfg.MySQLConfig.DBType),
				TargetDBVersion:   dbVersion,
				TargetTableName:   strings.ToUpper(table),
				TargetTableOption: strings.ToUpper(cfg.MySQLConfig.TableOption),
				SourceTableType:   tablesMap[table],
				SourceDBNLSSort:   nlsSort,
				SourceDBNLSComp:   nlsComp,
				Overwrite:         cfg.MySQLConfig.Overwrite,
				Oracle:            oracle,
				MySQL:             mysql,
			}
			tbl.OracleCollation = oraCollation
			if oraCollation {
				tbl.SourceSchemaCollation = schemaCollation
				tbl.SourceTableCollation = tblCollation[strings.ToUpper(table)]
			}
			tableChan <- tbl
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	close(tableChan)

	endTime = time.Now()
	zap.L().Info("gen oracle slice table finished",
		zap.String("schema", cfg.OracleConfig.SchemaName),
		zap.Int("table totals", len(exporters)),
		zap.Int("table gens", len(tables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return tables, nil
}

// O2M special
func (t *Table) GenTableSuffix(primaryColumns []string, singleIntegerPK bool) (tableSuffix string, err error) {
	var (
		tableCollation string
	)
	// schema、db、table collation
	if t.OracleCollation {
		// table collation
		if t.SourceTableCollation != "" {
			if val, ok := common.OracleCollationMap[t.SourceTableCollation]; ok {
				tableCollation = val
			} else {
				return tableSuffix, fmt.Errorf("oracle table collation [%v] isn't support", t.SourceTableCollation)
			}
		}
		// schema collation
		if t.SourceTableCollation == "" && t.SourceSchemaCollation != "" {
			if val, ok := common.OracleCollationMap[t.SourceSchemaCollation]; ok {
				tableCollation = val
			} else {
				return tableSuffix, fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support", t.SourceSchemaCollation, t.SourceTableCollation)
			}
		}
		if t.SourceTableName == "" && t.SourceSchemaCollation == "" {
			return tableSuffix, fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support", t.SourceSchemaCollation, t.SourceTableCollation)
		}
	} else {
		// db collation
		if val, ok := common.OracleCollationMap[t.SourceDBNLSComp]; ok {
			tableCollation = val
		} else {
			return tableSuffix, fmt.Errorf("oracle db nls_comp [%v] nls_sort [%v] isn't support", t.SourceDBNLSComp, t.SourceDBNLSSort)
		}
	}
	// table-option 表后缀可选项
	if t.TargetDBType == common.MySQLTargetDBType || t.TargetTableOption == "" {
		zap.L().Warn("reverse oracle table suffix",
			zap.String("table", t.String()),
			zap.String("table-option", "table-option is null, would be disabled"))
		// table suffix
		tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
			strings.ToLower(common.MySQLCharacterSet), tableCollation)

	} else {
		// TiDB
		clusteredIdxVal, err := t.MySQL.GetTiDBClusteredIndexValue()
		if err != nil {
			return tableSuffix, err
		}
		switch strings.ToUpper(clusteredIdxVal) {
		case common.TiDBClusteredIndexOFFValue:
			zap.L().Warn("reverse oracle table suffix",
				zap.String("table", t.String()),
				zap.String("tidb_enable_clustered_index", common.TiDBClusteredIndexOFFValue),
				zap.String("table-option", "tidb_enable_clustered_index is off, would be enabled"))

			if t.TargetTableOption != "" {
				tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s %s",
					strings.ToLower(common.MySQLCharacterSet), tableCollation, strings.ToUpper(t.TargetTableOption))
			} else {
				tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
					strings.ToLower(common.MySQLCharacterSet), tableCollation)
			}
		case common.TiDBClusteredIndexONValue:
			zap.L().Warn("reverse oracle table suffix",
				zap.String("table", t.String()),
				zap.String("tidb_enable_clustered_index", common.TiDBClusteredIndexONValue),
				zap.String("table-option", "tidb_enable_clustered_index is on, would be disabled"))

			tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
				strings.ToLower(common.MySQLCharacterSet), tableCollation)

		default:
			// tidb_enable_clustered_index = int_only / tidb_enable_clustered_index 不存在值，等于空
			pkVal, err := t.MySQL.GetTiDBAlterPKValue()
			if err != nil {
				return tableSuffix, err
			}
			if !fastjson.Exists([]byte(pkVal), "alter-primary-key") {
				zap.L().Warn("reverse oracle table suffix",
					zap.String("table", t.String()),
					zap.String("tidb_enable_clustered_index", strings.ToUpper(clusteredIdxVal)),
					zap.String("alter-primary-key", "not exist"),
					zap.String("table-option", "alter-primary-key isn't exits, would be disable"))

				tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
					strings.ToLower(common.MySQLCharacterSet), tableCollation)

			} else {
				var p fastjson.Parser
				v, err := p.Parse(pkVal)
				if err != nil {
					return tableSuffix, err
				}

				isAlterPK := v.GetBool("alter-primary-key")

				// alter-primary-key = false
				// 整型主键 table-option 不生效
				// 单列主键是整型
				if !isAlterPK && len(primaryColumns) == 1 && singleIntegerPK {
					zap.L().Warn("reverse oracle table suffix",
						zap.String("table", t.String()),
						zap.String("tidb_enable_clustered_index", strings.ToUpper(clusteredIdxVal)),
						zap.Bool("alter-primary-key", isAlterPK),
						zap.String("table-option", "integer primary key, would be disable"))

					tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
						strings.ToLower(common.MySQLCharacterSet), tableCollation)

				} else {
					// table-option 生效
					// alter-primary-key = true
					// alter-primary-key = false && 联合主键 len(pkINFO)>1
					// alter-primary-key = false && 非整型主键
					if isAlterPK || (!isAlterPK && len(primaryColumns) > 1) || (!isAlterPK && !singleIntegerPK) {
						zap.L().Warn("reverse oracle table suffix",
							zap.String("table", t.String()),
							zap.String("tidb_enable_clustered_index", strings.ToUpper(clusteredIdxVal)),
							zap.Bool("alter-primary-key", isAlterPK),
							zap.String("table-option", "enabled"))

						if t.TargetTableOption != "" {
							tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s %s",
								strings.ToLower(common.MySQLCharacterSet), tableCollation, strings.ToUpper(t.TargetTableOption))
						} else {
							tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
								strings.ToLower(common.MySQLCharacterSet), tableCollation)
						}
					} else {
						zap.L().Error("reverse oracle table suffix",
							zap.String("table", t.String()),
							zap.String("tidb_enable_clustered_index", strings.ToUpper(clusteredIdxVal)),
							zap.Bool("alter-primary-key", isAlterPK),
							zap.String("table-option", "disabled"),
							zap.Error(fmt.Errorf("not support")))
						return tableSuffix, fmt.Errorf("reverse oracle table suffix error: table-option not support")
					}
				}
			}
		}
	}
	zap.L().Info("reverse oracle table suffix",
		zap.String("table", t.String()),
		zap.String("create table suffix", tableSuffix))

	return tableSuffix, nil
}

// 判断 Oracle 主键是否是整型主键
func (t *Table) IsSingleIntegerPK(primaryColumns []string, columnMetas []string) bool {
	singleIntegerPK := false

	// 单列主键且整型主键
	if len(primaryColumns) == 1 {
		// 单列主键数据类型获取判断
		for _, columnMeta := range columnMetas {
			columnName := strings.Fields(columnMeta)[0]
			columnType := strings.Fields(columnMeta)[1]

			if strings.EqualFold(primaryColumns[0], columnName) {
				// Map 规则转换后的字段对应数据类型
				// columnMeta 视角 columnName columnType ....
				for _, integerType := range common.TiDBIntegerPrimaryKeyList {
					if find := strings.Contains(strings.ToUpper(columnType), strings.ToUpper(integerType)); find {
						singleIntegerPK = true
					}
				}
			}
		}
	}
	return singleIntegerPK
}

func (t *Table) GetTablePrimaryKey() ([]map[string]string, error) {
	return t.Oracle.GetOracleSchemaTablePrimaryKey(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableUniqueKey() ([]map[string]string, error) {
	return t.Oracle.GetOracleSchemaTableUniqueKey(t.SourceSchemaName, t.SourceTableName)

}

func (t *Table) GetTableForeignKey() ([]map[string]string, error) {
	return t.Oracle.GetOracleSchemaTableForeignKey(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableCheckKey() ([]map[string]string, error) {
	return t.Oracle.GetOracleSchemaTableForeignKey(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableUniqueIndex() ([]map[string]string, error) {
	// 唯一索引
	return t.Oracle.GetOracleSchemaTableUniqueIndex(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableNormalIndex() ([]map[string]string, error) {
	// 普通索引【普通索引、函数索引、位图索引、DOMAIN 索引】
	return t.Oracle.GetOracleSchemaTableNormalIndex(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableComment() ([]map[string]string, error) {
	return t.Oracle.GetOracleSchemaTableComment(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) GetTableColumnMeta() ([]map[string]string, error) {
	// 获取表数据字段列信息
	return t.Oracle.GetOracleSchemaTableColumn(t.SourceSchemaName, t.SourceTableName, t.OracleCollation)
}

func (t *Table) GetTableColumnComment() ([]map[string]string, error) {
	// 获取表数据字段列备注
	return t.Oracle.GetOracleSchemaTableColumnComment(t.SourceSchemaName, t.SourceTableName)
}

func (t *Table) String() string {
	jsonStr, _ := json.Marshal(t)
	return string(jsonStr)
}

func GenCreateSchema(f *reverse.File, sourceSchema, targetSchema, nlsComp string) error {
	startTime := time.Now()
	var (
		sqlRev          strings.Builder
		schemaCollation string
	)

	oraDBVersion, err := f.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}

	oraCollation := false
	if common.VersionOrdinal(oraDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oraCollation = true
	}
	if oraCollation {
		schemaCollation, err = f.Oracle.GetOracleSchemaCollation(sourceSchema)
		if err != nil {
			return err
		}
	}

	sqlRev.WriteString("/*\n")
	sqlRev.WriteString(" oracle schema reverse mysql database\n")
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
	t.AppendRows([]table.Row{
		{"Schema", sourceSchema, targetSchema, "Create Schema"},
	})
	sqlRev.WriteString(t.Render() + "\n")
	sqlRev.WriteString("*/\n")

	if oraCollation {
		if _, ok := common.OracleCollationMap[strings.ToUpper(schemaCollation)]; !ok {
			return fmt.Errorf("oracle schema collation [%s] isn't support", schemaCollation)
		}
		sqlRev.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET %s COLLATE %s;\n\n", strings.ToUpper(targetSchema), strings.ToLower(common.MySQLCharacterSet), common.OracleCollationMap[strings.ToUpper(schemaCollation)]))
	} else {
		if _, ok := common.OracleCollationMap[strings.ToUpper(nlsComp)]; !ok {
			return fmt.Errorf("oracle db nls_comp collation [%s] isn't support", nlsComp)
		}
		sqlRev.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET %s COLLATE %s;\n\n", strings.ToUpper(targetSchema), strings.ToLower(common.MySQLCharacterSet), common.OracleCollationMap[strings.ToUpper(nlsComp)]))
	}

	if _, err = f.RWriteString(sqlRev.String()); err != nil {
		return err
	}
	endTime := time.Now()
	zap.L().Info("output oracle to mysql schema create sql",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func GenCompatibilityTable(f *reverse.File, sourceSchema string, partitionTables, temporaryTables, clusteredTables []string) error {
	startTime := time.Now()
	// 兼容提示
	if len(partitionTables) > 0 || len(temporaryTables) > 0 || len(clusteredTables) > 0 {
		var sqlComp strings.Builder

		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" oracle table maybe mysql has compatibility, will convert to normal table, please manual process\n")
		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"SCHEMA", "TABLE NAME", "ORACLE TABLE TYPE", "SUGGEST"})

		if len(partitionTables) > 0 {
			for _, part := range partitionTables {
				t.AppendRows([]table.Row{
					{sourceSchema, part, "Partition", "Manual Process Table"},
				})
			}
		}
		if len(temporaryTables) > 0 {
			for _, temp := range temporaryTables {
				t.AppendRows([]table.Row{
					{sourceSchema, temp, "Temporary", "Manual Process Table"},
				})
			}
		}
		if len(clusteredTables) > 0 {
			for _, cd := range clusteredTables {
				t.AppendRows([]table.Row{
					{sourceSchema, cd, "Clustered", "Manual Process Table"},
				})
			}
		}
		sqlComp.WriteString(t.Render() + "\n")
		sqlComp.WriteString("*/\n")

		if _, err := f.CWriteString(sqlComp.String()); err != nil {
			return err
		}
	}
	endTime := time.Now()
	zap.L().Info("output oracle to mysql compatibility tips",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
