/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed r. in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package public

import (
	"context"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

type Change struct {
	Ctx              context.Context `json:"-"`
	DBTypeS          string          `json:"db_type_s"`
	DBTypeT          string          `json:"db_type_t"`
	SourceSchemaName string          `json:"source_schema_name"`
	TargetSchemaName string          `json:"target_schema_name"`
	SourceTables     []string        `json:"source_tables"`
	Threads          int             `json:"threads"`
	OracleCollation  bool            `json:"oracle_collation"`
	Oracle           *oracle.Oracle  `json:"-"`
	MetaDB           *meta.Meta      `json:"-"`
}

func (r *Change) ChangeTableName() (map[string]string, error) {
	startTime := time.Now()
	tableNameRule := make(map[string]string)
	customTableNameRule := make(map[string]string)
	// 获取表名自定义规则
	tableNameRules, err := meta.NewTableNameRuleModel(r.MetaDB).DetailTableNameRule(r.Ctx, &meta.TableNameRule{
		DBTypeS:     r.DBTypeS,
		DBTypeT:     r.DBTypeT,
		SchemaNameS: r.SourceSchemaName,
		SchemaNameT: r.TargetSchemaName,
	})
	if err != nil {
		return tableNameRule, err
	}

	if len(tableNameRules) > 0 {
		for _, tr := range tableNameRules {
			customTableNameRule[common.StringUPPER(tr.TableNameS)] = common.StringUPPER(tr.TableNameT)
		}
	}

	wg := &sync.WaitGroup{}
	tableChS := make(chan string, common.ChannelBufferSize)
	tableChT := make(chan string, common.ChannelBufferSize)
	done := make(chan struct{})

	go func(done func()) {
		for c := range tableChT {
			if val, ok := customTableNameRule[common.StringUPPER(c)]; ok {
				tableNameRule[common.StringUPPER(c)] = val
			} else {
				tableNameRule[common.StringUPPER(c)] = common.StringUPPER(c)
			}
		}
		done()
	}(func() {
		done <- struct{}{}
	})

	go func() {
		for _, sourceTable := range r.SourceTables {
			tableChS <- sourceTable
		}
		close(tableChS)
	}()

	for i := 0; i < r.Threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range tableChS {
				tableChT <- c
			}
		}()
	}
	wg.Wait()
	close(tableChT)
	<-done

	zap.L().Warn("get source table name mapping rules",
		zap.String("schema", r.SourceSchemaName),
		zap.String("cost", time.Now().Sub(startTime).String()))

	return tableNameRule, nil
}

// 数据库查询获取自定义表结构转换规则
// 加载数据类型转换规则【处理字段级别、表级别、库级别数据类型映射规则】
// 数据类型转换规则判断，未设置自定义规则，默认采用内置默认字段类型转换
func (r *Change) ChangeTableColumnDatatype() (map[string]map[string]string, error) {
	startTime := time.Now()
	tableDatatypeMap := make(map[string]map[string]string)

	// 获取内置字段数据类型名映射规则
	buildinDatatypeNames, err := meta.NewBuildinDatatypeRuleModel(r.MetaDB).BatchQueryBuildinDatatype(r.Ctx, &meta.BuildinDatatypeRule{
		DBTypeS: r.DBTypeS,
		DBTypeT: r.DBTypeT,
	})
	if err != nil {
		return tableDatatypeMap, err
	}

	// 获取自定义 schema 级别数据类型映射规则
	schemaDataTypeMapSlice, err := meta.NewSchemaDatatypeRuleModel(r.MetaDB).DetailSchemaRule(r.Ctx, &meta.SchemaDatatypeRule{
		DBTypeS:     r.DBTypeS,
		DBTypeT:     r.DBTypeT,
		SchemaNameS: r.SourceSchemaName,
	})
	if err != nil {
		return tableDatatypeMap, err
	}

	// 获取自定义 table 级别数据类型映射规则
	tableDataTypeMapSlice, err := meta.NewTableDatatypeRuleModel(r.MetaDB).DetailTableRule(r.Ctx, &meta.TableDatatypeRule{
		DBTypeS:     r.DBTypeS,
		DBTypeT:     r.DBTypeT,
		SchemaNameS: r.SourceSchemaName,
	})
	if err != nil {
		return tableDatatypeMap, err
	}

	// 获取自定义字段数据类型映射规则
	columnDataTypeMapSlice, err := meta.NewColumnDatatypeRuleModel(r.MetaDB).DetailColumnRule(r.Ctx, &meta.ColumnDatatypeRule{
		DBTypeS:     r.DBTypeS,
		DBTypeT:     r.DBTypeT,
		SchemaNameS: r.SourceSchemaName,
	})
	if err != nil {
		return tableDatatypeMap, err
	}

	wg := &errgroup.Group{}
	wg.SetLimit(r.Threads)

	tableColumnChan := make(chan map[string]map[string]string, common.ChannelBufferSize)
	done := make(chan struct{})

	go func(done func()) {
		for c := range tableColumnChan {
			for key, val := range c {
				tableDatatypeMap[key] = val
			}
		}
		done()
	}(func() {
		done <- struct{}{}
	})

	for _, table := range r.SourceTables {
		sourceTable := table
		wg.Go(func() error {
			// 获取表字段信息
			tableColumnINFO, err := r.Oracle.GetOracleSchemaTableColumn(r.SourceSchemaName, sourceTable, r.OracleCollation)
			if err != nil {
				return err
			}

			columnDatatypeMap := make(map[string]string, 1)
			tableDatatypeTempMap := make(map[string]map[string]string, 1)

			for _, rowCol := range tableColumnINFO {
				originColumnType, buildInColumnType, err := OracleTableColumnMapMySQLRule(r.SourceSchemaName, sourceTable, Column{
					DataType:   rowCol["DATA_TYPE"],
					CharUsed:   rowCol["CHAR_USED"],
					CharLength: rowCol["CHAR_LENGTH"],
					ColumnInfo: ColumnInfo{
						DataLength:    rowCol["DATA_LENGTH"],
						DataPrecision: rowCol["DATA_PRECISION"],
						DataScale:     rowCol["DATA_SCALE"],
						NULLABLE:      rowCol["NULLABLE"],
						DataDefault:   rowCol["DATA_DEFAULT"],
						Comment:       rowCol["COMMENTS"],
					},
				}, buildinDatatypeNames)
				if err != nil {
					return err
				}

				// 优先级
				// column > table > schema > buildin
				if len(columnDataTypeMapSlice) == 0 {
					columnDatatypeMap[rowCol["COLUMN_NAME"]] = LoadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)
				}

				// only column rule
				columnTypeFromColumn := LoadColumnTypeRuleOnlyUsingColumn(rowCol["COLUMN_NAME"], originColumnType, buildInColumnType, columnDataTypeMapSlice)

				// table or schema rule check, return column type
				columnTypeFromOther := LoadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

				// column or other rule check, return column type
				switch {
				case columnTypeFromColumn != buildInColumnType && columnTypeFromOther == buildInColumnType:
					columnDatatypeMap[rowCol["COLUMN_NAME"]] = common.StringUPPER(columnTypeFromColumn)
				case columnTypeFromColumn != buildInColumnType && columnTypeFromOther != buildInColumnType:
					columnDatatypeMap[rowCol["COLUMN_NAME"]] = common.StringUPPER(columnTypeFromColumn)
				case columnTypeFromColumn == buildInColumnType && columnTypeFromOther != buildInColumnType:
					columnDatatypeMap[rowCol["COLUMN_NAME"]] = common.StringUPPER(columnTypeFromOther)
				default:
					columnDatatypeMap[rowCol["COLUMN_NAME"]] = common.StringUPPER(buildInColumnType)
				}
			}

			tableDatatypeTempMap[sourceTable] = columnDatatypeMap

			tableColumnChan <- tableDatatypeTempMap
			return nil
		})
	}

	if err = wg.Wait(); err != nil {
		return nil, err
	}
	close(tableColumnChan)
	<-done

	zap.L().Warn("get source table column datatype mapping rules",
		zap.String("schema", r.SourceSchemaName),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return tableDatatypeMap, nil
}

func (r *Change) ChangeTableColumnDefaultValue() (map[string]map[string]string, error) {
	startTime := time.Now()
	tableDefaultValMap := make(map[string]map[string]string)
	// 获取内置字段默认值映射规则 -> global
	globalDefaultValueMapSlice, err := meta.NewBuildinGlobalDefaultvalModel(r.MetaDB).DetailGlobalDefaultVal(r.Ctx, &meta.BuildinGlobalDefaultval{
		DBTypeS: r.DBTypeS,
		DBTypeT: r.DBTypeT,
	})
	if err != nil {
		return tableDefaultValMap, err
	}
	// 获取自定义字段默认值映射规则
	columnDefaultValueMapSlice, err := meta.NewBuildinColumnDefaultvalModel(r.MetaDB).DetailColumnDefaultVal(r.Ctx, &meta.BuildinColumnDefaultval{
		DBTypeS:     r.DBTypeS,
		DBTypeT:     r.DBTypeT,
		SchemaNameS: r.SourceSchemaName,
	})
	if err != nil {
		return tableDefaultValMap, err
	}

	wg := &errgroup.Group{}
	wg.SetLimit(r.Threads)

	columnDefaultChan := make(chan map[string]map[string]string, common.ChannelBufferSize)
	done := make(chan struct{})

	go func(done func()) {
		for c := range columnDefaultChan {
			for key, val := range c {
				tableDefaultValMap[key] = val
			}
		}
		done()
	}(func() {
		done <- struct{}{}
	})

	for _, table := range r.SourceTables {
		sourceTable := table
		wg.Go(func() error {
			// 获取表字段信息
			tableColumnINFO, err := r.Oracle.GetOracleSchemaTableColumn(r.SourceSchemaName, sourceTable, r.OracleCollation)
			if err != nil {
				return err
			}

			columnDataDefaultValMap := make(map[string]string, 1)
			tableDefaultValTempMap := make(map[string]map[string]string, 1)

			for _, rowCol := range tableColumnINFO {
				// 优先级
				// column > global
				columnDataDefaultValMap[rowCol["COLUMN_NAME"]], err = LoadColumnDefaultValueRule(
					rowCol["COLUMN_NAME"], rowCol["DATA_DEFAULT"], columnDefaultValueMapSlice, globalDefaultValueMapSlice)
				if err != nil {
					return err
				}
			}

			tableDefaultValTempMap[sourceTable] = columnDataDefaultValMap
			columnDefaultChan <- tableDefaultValTempMap
			return nil
		})
	}
	if err = wg.Wait(); err != nil {
		return nil, err
	}
	close(columnDefaultChan)
	<-done

	zap.L().Warn("get source table column default value mapping rules",
		zap.String("schema", r.SourceSchemaName),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return tableDefaultValMap, nil
}
