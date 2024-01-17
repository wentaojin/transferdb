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
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/reverse"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Table struct {
	Ctx                   context.Context `json:"-"`
	SourceSchemaName      string          `json:"source_schema_name"`
	TargetSchemaName      string          `json:"target_schema_name"`
	SourceTableName       string          `json:"source_table_name"`
	TargetDBVersion       string          `json:"target_db_version"`
	TargetTableName       string          `json:"target_table_name"`
	OracleCollation       bool            `json:"oracle_collation"`
	SourceDBCharset       string          `json:"sourcedb_charset"`
	TargetDBCharset       string          `json:"targetdb_charset"`
	SourceSchemaCollation string          `json:"source_schema_collation"` // 可为空
	SourceTableCollation  string          `json:"source_table_collation"`  // 可为空
	SourceDBNLSSort       string          `json:"sourcedb_nlssort"`
	SourceDBNLSComp       string          `json:"sourcedb_nlscomp"`
	SourceTableType       string          `json:"source_table_type"`
	LowerCaseFieldName    string          `json:"lower_case_field_name"`

	TableColumnDatatypeRule         map[string]string `json:"table_column_datatype_rule"`
	TableColumnDefaultValRule       map[string]string `json:"table_column_default_val_rule"`
	TableColumnDefaultValSourceRule map[string]bool   `json:"table_column_default_val_source_rule"` // 判断表字段 defaultVal 来源于 database or custom

	Overwrite bool           `json:"overwrite"`
	Oracle    *oracle.Oracle `json:"-"`
	MySQL     *mysql.MySQL   `json:"-"`
	MetaDB    *meta.Meta     `json:"-"`
}

func GenReverseTableTask(r *Reverse, tableNameRule map[string]string, tableColumnRule map[string]map[string]string, tableDefaultSourceRule map[string]map[string]bool, tableDefaultRule map[string]map[string]string, oracleDBVersion, oracleDBCharset, targetDBCharset string, oracleCollation bool, lowerCaseFieldName string, exporters []string, nlsSort, nlsComp string) ([]*Table, error) {
	var tables []*Table

	beginTime := time.Now()
	defer func() {
		endTime := time.Now()
		zap.L().Info("gen oracle table list finished",
			zap.String("schema", r.Cfg.SchemaConfig.SourceSchema),
			zap.Int("table totals", len(exporters)),
			zap.Int("table gens", len(tables)),
			zap.String("cost", endTime.Sub(beginTime).String()))
	}()

	endTime := time.Now()
	zap.L().Info("get oracle db character and version finished",
		zap.String("schema", r.Cfg.SchemaConfig.SourceSchema),
		zap.String("db version", oracleDBVersion),
		zap.String("db character", oracleDBCharset),
		zap.Int("table totals", len(exporters)),
		zap.Bool("table collation", oracleCollation))

	var (
		tblCollation    map[string]string
		schemaCollation string
		err             error
	)

	if oracleCollation {
		startTime := time.Now()
		schemaCollation, err = r.Oracle.GetOracleSchemaCollation(r.Cfg.SchemaConfig.SourceSchema)
		if err != nil {
			return tables, err
		}
		tblCollation, err = r.Oracle.GetOracleSchemaTableCollation(r.Cfg.SchemaConfig.SourceSchema, schemaCollation)
		if err != nil {
			return tables, err
		}
		endTime = time.Now()
		zap.L().Info("get oracle schema and table collation finished",
			zap.String("schema", r.Cfg.SchemaConfig.SourceSchema),
			zap.String("db version", oracleDBVersion),
			zap.String("db character", oracleDBCharset),
			zap.Int("table totals", len(exporters)),
			zap.Bool("table collation", oracleCollation),
			zap.String("cost", endTime.Sub(startTime).String()))
	}

	startTime := time.Now()
	tablesMap, err := r.Oracle.GetOracleSchemaTableType(r.Cfg.SchemaConfig.SourceSchema)
	if err != nil {
		return tables, err
	}
	endTime = time.Now()
	zap.L().Info("get oracle table type finished",
		zap.String("schema", r.Cfg.SchemaConfig.SourceSchema),
		zap.String("db version", oracleDBVersion),
		zap.String("db character", oracleDBCharset),
		zap.Int("table totals", len(exporters)),
		zap.Bool("table collation", oracleCollation),
		zap.String("cost", endTime.Sub(startTime).String()))

	// 获取 MySQL 版本
	mysqlVersion, err := r.Mysql.GetMySQLDBVersion()
	if err != nil {
		return nil, err
	}

	var dbVersion string

	if strings.Contains(mysqlVersion, common.MySQLVersionDelimiter) {
		dbVersion = strings.Split(mysqlVersion, common.MySQLVersionDelimiter)[0]
	} else {
		dbVersion = mysqlVersion
	}

	startTime = time.Now()
	g1 := &errgroup.Group{}
	tableChan := make(chan *Table, common.ChannelBufferSize)

	g1.Go(func() error {
		g2 := &errgroup.Group{}
		g2.SetLimit(r.Cfg.ReverseConfig.ReverseThreads)
		for _, exporter := range exporters {
			t := exporter
			g2.Go(func() error {
				// 库名、表名规则
				var targetTableName string
				if val, ok := tableNameRule[common.StringUPPER(t)]; ok {
					targetTableName = val
				} else {
					targetTableName = common.StringUPPER(t)
				}

				tbl := &Table{
					Ctx:                             r.Ctx,
					SourceSchemaName:                r.Cfg.SchemaConfig.SourceSchema,
					TargetSchemaName:                common.StringUPPER(r.Cfg.SchemaConfig.TargetSchema),
					SourceTableName:                 t,
					TargetDBVersion:                 dbVersion,
					TargetTableName:                 targetTableName,
					SourceTableType:                 tablesMap[t],
					SourceDBCharset:                 oracleDBCharset,
					TargetDBCharset:                 targetDBCharset,
					SourceDBNLSSort:                 nlsSort,
					SourceDBNLSComp:                 nlsComp,
					LowerCaseFieldName:              lowerCaseFieldName,
					TableColumnDatatypeRule:         tableColumnRule[common.StringUPPER(t)],
					TableColumnDefaultValRule:       tableDefaultRule[common.StringUPPER(t)],
					TableColumnDefaultValSourceRule: tableDefaultSourceRule[common.StringUPPER(t)],
					Overwrite:                       r.Cfg.MySQLConfig.Overwrite,
					Oracle:                          r.Oracle,
					MySQL:                           r.Mysql,
					MetaDB:                          r.MetaDB,
				}
				tbl.OracleCollation = oracleCollation
				if oracleCollation {
					tbl.SourceSchemaCollation = schemaCollation
					tbl.SourceTableCollation = tblCollation[common.StringUPPER(t)]
				}
				tableChan <- tbl
				return nil
			})
		}

		err = g2.Wait()
		if err != nil {
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

	endTime = time.Now()
	zap.L().Info("gen oracle slice table finished",
		zap.String("schema", r.Cfg.SchemaConfig.SourceSchema),
		zap.Int("table totals", len(exporters)),
		zap.Int("table gens", len(tables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return tables, nil
}

func (t *Table) GetTablePrimaryKey() ([]map[string]string, error) {
	primaryKeyMap, err := t.Oracle.GetOracleSchemaTablePrimaryKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}

	var newMap []map[string]string
	for _, m := range primaryKeyMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table primary key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table primary key [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table primary key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table primary key [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
}

func (t *Table) GetTableUniqueKey() ([]map[string]string, error) {
	uniKeyMap, err := t.Oracle.GetOracleSchemaTableUniqueKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}

	var newMap []map[string]string
	for _, m := range uniKeyMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table unique key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table unique key [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table unique key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table unique key [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
}

func (t *Table) GetTableForeignKey() ([]map[string]string, error) {
	forignkMap, err := t.Oracle.GetOracleSchemaTableForeignKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}

	var newMap []map[string]string
	for _, m := range forignkMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table foreign key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table foreign key [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table foreign key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table foreign key [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil

}

func (t *Table) GetTableCheckKey() ([]map[string]string, error) {
	checkKmap, err := t.Oracle.GetOracleSchemaTableCheckKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}

	var newMap []map[string]string
	for _, m := range checkKmap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table check key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table check key [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table check key [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table check key [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
}

func (t *Table) GetTableUniqueIndex() ([]map[string]string, error) {
	// 唯一索引
	uniqIdxMap, err := t.Oracle.GetOracleSchemaTableUniqueIndex(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}

	var newMap []map[string]string
	for _, m := range uniqIdxMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table unique index [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table unique index [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table unique index [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table unique index [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
}

func (t *Table) GetTableNormalIndex() ([]map[string]string, error) {
	// 普通索引【普通索引、函数索引、位图索引、DOMAIN 索引】
	normalMap, err := t.Oracle.GetOracleSchemaTableNormalIndex(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}
	var newMap []map[string]string
	for _, m := range normalMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table normal index [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table normal index [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table normal index [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table normal index [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
}

func (t *Table) GetTableComment() ([]map[string]string, error) {
	commetMap, err := t.Oracle.GetOracleSchemaTableComment(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}
	var newMap []map[string]string
	for _, m := range commetMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table comment [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table comment [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table comment [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table comment [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
}

func (t *Table) GetTableColumnMeta() ([]map[string]string, error) {
	// 获取表数据字段列信息
	columnMap, err := t.Oracle.GetOracleSchemaTableColumn(t.SourceSchemaName, t.SourceTableName, t.OracleCollation)
	if err != nil {
		return nil, err
	}
	var newMap []map[string]string
	for _, m := range columnMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table column [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table column [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table column [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table column [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
}

func (t *Table) GetTableColumnComment() ([]map[string]string, error) {
	// 获取表数据字段列备注
	commentMap, err := t.Oracle.GetOracleSchemaTableColumnComment(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return nil, err
	}
	var newMap []map[string]string
	for _, m := range commentMap {
		kmap := make(map[string]string)
		for key, val := range m {
			convUtf8Raw, err := common.CharsetConvert([]byte(key), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table column comment [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table column comment [%v] charset convert failed, %v", m, err)
			}

			key = string(convTargetRaw)

			convUtf8Raw, err = common.CharsetConvert([]byte(val), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("table column comment [%v] charset convert failed, %v", m, err)
			}

			convTargetRaw, err = common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
			if err != nil {
				return nil, fmt.Errorf("table column comment [%v] charset convert failed, %v", m, err)
			}
			kmap[key] = string(convTargetRaw)
		}
		newMap = append(newMap, kmap)
	}
	return newMap, nil
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

	ddl, err := t.GetTableOriginDDL()
	if err != nil {
		return nil, err
	}

	return &Info{
		SourceTableDDL:    ddl,
		PrimaryKeyINFO:    primaryKey,
		UniqueKeyINFO:     uniqueKey,
		ForeignKeyINFO:    foreignKey,
		CheckKeyINFO:      checkKey,
		UniqueIndexINFO:   uniqueIndex,
		NormalIndexINFO:   normalIndex,
		TableCommentINFO:  tableComment,
		TableColumnINFO:   columnMeta,
		ColumnCommentINFO: columnComment,
	}, nil
}

func (t *Table) GetTableOriginDDL() (string, error) {
	ddl, err := t.Oracle.GetOracleTableOriginDDL(t.SourceSchemaName, t.SourceTableName, "TABLE")
	if err != nil {
		return ddl, err
	}

	convertUtf8Raw, err := common.CharsetConvert([]byte(ddl), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.SourceDBCharset)], common.CharsetUTF8MB4)
	if err != nil {
		return ddl, fmt.Errorf("table [%v] ddl charset convert failed, %v", t.SourceTableName, err)
	}

	convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(t.TargetDBCharset)])
	if err != nil {
		return ddl, fmt.Errorf("table [%v] ddl charset convert failed, %v", t.SourceTableName, err)
	}

	return string(convertTargetRaw), nil
}

func (t *Table) String() string {
	jsonStr, _ := json.Marshal(t)
	return string(jsonStr)
}

func GenCreateSchema(w *reverse.Write, lowerCaseFieldName, sourceSchema, targetSchema, sourceDBCharset, nlsComp string, directWrite bool) error {
	startTime := time.Now()
	var (
		sqlRev          strings.Builder
		schemaCollation string
	)

	oraDBVersion, err := w.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}

	oraCollation := false
	if common.VersionOrdinal(oraDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oraCollation = true
	}
	if oraCollation {
		schemaCollation, err = w.Oracle.GetOracleSchemaCollation(sourceSchema)
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

	targetDBCharset := common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeOracle2MySQL][sourceDBCharset]

	// 库名大小写
	if strings.EqualFold(lowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
		targetSchema = strings.ToLower(targetSchema)
	}
	if strings.EqualFold(lowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
		targetSchema = strings.ToUpper(targetSchema)
	}

	if oraCollation {
		targetSchemaCollation, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeOracle2MySQL][common.StringUPPER(schemaCollation)][targetDBCharset]
		if !ok {
			return fmt.Errorf("oracle schema collation [%s] isn't support", schemaCollation)
		}
		sqlRev.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET %s COLLATE %s;\n\n", targetSchema, targetDBCharset, targetSchemaCollation))
	} else {
		targetSchemaCollation, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeOracle2MySQL][common.StringUPPER(nlsComp)][targetDBCharset]
		if !ok {
			return fmt.Errorf("oracle db nls_comp collation [%s] isn't support", nlsComp)
		}
		sqlRev.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET %s COLLATE %s;\n\n", targetSchema, targetDBCharset, targetSchemaCollation))
	}

	if directWrite {
		err = w.RWriteDB(sqlRev.String())
		if err != nil {
			return err
		}
	} else {
		if _, err = w.RWriteFile(sqlRev.String()); err != nil {
			return err
		}
	}
	endTime := time.Now()
	zap.L().Info("output oracle to mysql schema create sql",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func GenCompatibilityTable(f *reverse.Write, sourceSchema string, partitionTables, temporaryTables, clusteredTables []string, materializedViews []string) error {
	startTime := time.Now()
	// 兼容提示
	if len(partitionTables) > 0 || len(temporaryTables) > 0 || len(clusteredTables) > 0 || len(materializedViews) > 0 {
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

		if _, err := f.CWriteFile(sqlComp.String()); err != nil {
			return err
		}

		if len(materializedViews) > 0 {
			var mviewComp strings.Builder

			mviewComp.WriteString("/*\n")
			mviewComp.WriteString(" oracle materialized view maybe mysql has compatibility, will skip convert to reverse, please manual process\n")
			t = table.NewWriter()
			t.SetStyle(table.StyleLight)
			t.AppendHeader(table.Row{"SCHEMA", "MVIEW NAME", "ORACLE TABLE TYPE", "SUGGEST"})

			for _, cd := range materializedViews {
				t.AppendRows([]table.Row{
					{sourceSchema, cd, "Materialized View", "Manual Process Table"},
				})
			}

			mviewComp.WriteString(t.Render() + "\n")
			mviewComp.WriteString("*/\n")

			if _, err := f.CWriteFile(mviewComp.String()); err != nil {
				return err
			}
		}
	}
	endTime := time.Now()
	zap.L().Info("output oracle to mysql compatibility tips",
		zap.String("schema", sourceSchema),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
