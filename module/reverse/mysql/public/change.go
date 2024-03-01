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
package public

import (
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"regexp"
	"strings"
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
	SourceDBCharset  string          `json:"source_db_charset"`
	TargetDBCharset  string          `json:"target_db_charset"`
	Threads          int             `json:"threads"`
	MySQL            *mysql.MySQL    `json:"-"`
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
			tableNameRule[common.StringUPPER(tr.TableNameS)] = common.StringUPPER(tr.TableNameT)
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

	zap.L().Warn("get source table column table mapping rules",
		zap.String("schema", r.SourceSchemaName),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return tableNameRule, nil
}

// 数据库查询获取自定义表结构转换规则
// 加载数据类型转换规则【处理字段级别、表级别、库级别数据类型映射规则】
// 数据类型转换规则判断，未设置自定义规则，默认采用内置默认字段类型转换
func (r *Change) ChangeTableColumnDatatype() (map[string]map[string]string, error) {
	startTime := time.Now()
	tableColumnDatatypeMap := make(map[string]map[string]string)

	// 获取内置映射规则
	buildinDatatypeNames, err := meta.NewBuildinDatatypeRuleModel(r.MetaDB).BatchQueryBuildinDatatype(r.Ctx, &meta.BuildinDatatypeRule{
		DBTypeS: r.DBTypeS,
		DBTypeT: r.DBTypeT,
	})
	if err != nil {
		return tableColumnDatatypeMap, err
	}

	// 获取自定义 schema 级别数据类型映射规则
	schemaDataTypeMapSlice, err := meta.NewSchemaDatatypeRuleModel(r.MetaDB).DetailSchemaRule(r.Ctx, &meta.SchemaDatatypeRule{
		DBTypeS:     r.DBTypeS,
		DBTypeT:     r.DBTypeT,
		SchemaNameS: r.SourceSchemaName,
	})
	if err != nil {
		return tableColumnDatatypeMap, err
	}

	wg := &errgroup.Group{}
	wg.SetLimit(r.Threads)

	tableColumnChan := make(chan map[string]map[string]string, common.ChannelBufferSize)
	done := make(chan struct{})

	go func(done func()) {
		for c := range tableColumnChan {
			for key, val := range c {
				tableColumnDatatypeMap[key] = val
			}
		}
		done()
	}(func() {
		done <- struct{}{}
	})

	for _, table := range r.SourceTables {
		sourceTable := table
		wg.Go(func() error {
			// 获取自定义 table 级别数据类型映射规则
			tableDataTypeMapSlice, err := meta.NewTableDatatypeRuleModel(r.MetaDB).DetailTableRule(r.Ctx, &meta.TableDatatypeRule{
				DBTypeS:     r.DBTypeS,
				DBTypeT:     r.DBTypeT,
				SchemaNameS: r.SourceSchemaName,
				TableNameS:  sourceTable,
			})
			if err != nil {
				return err
			}

			// 获取表字段信息
			tableColumnINFO, err := r.MySQL.GetMySQLTableColumn(r.SourceSchemaName, sourceTable)
			if err != nil {
				return err
			}

			columnDatatypeMap := make(map[string]string, 1)
			tableDatatypeTempMap := make(map[string]map[string]string, 1)

			for _, rowCol := range tableColumnINFO {
				originColumnType, buildInColumnType, err := MySQLTableColumnMapOracleRule(r.SourceSchemaName, sourceTable, Column{
					DataType: rowCol["DATA_TYPE"],
					ColumnInfo: ColumnInfo{
						DataLength:        rowCol["DATA_LENGTH"],
						DataPrecision:     rowCol["DATA_PRECISION"],
						DataScale:         rowCol["DATA_SCALE"],
						DatetimePrecision: rowCol["DATETIME_PRECISION"],
						NULLABLE:          rowCol["NULLABLE"],
						DataDefault:       rowCol["DATA_DEFAULT"],
						Comment:           rowCol["COMMENTS"],
					},
				}, buildinDatatypeNames)
				if err != nil {
					return err
				}

				// charset
				columnName := rowCol["COLUMN_NAME"]
				convUtf8Raw, err := common.CharsetConvert([]byte(columnName), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(r.SourceDBCharset)], common.CharsetUTF8MB4)
				if err != nil {
					return fmt.Errorf("table column name rule [%s] charset convert failed, %v", columnName, err)
				}

				// 获取自定义 column 映射规则
				columnDataTypeMapSlice, err := meta.NewColumnDatatypeRuleModel(r.MetaDB).DetailColumnRule(r.Ctx, &meta.ColumnDatatypeRule{
					DBTypeS:     r.DBTypeS,
					DBTypeT:     r.DBTypeT,
					SchemaNameS: r.SourceSchemaName,
					ColumnNameS: columnName,
				})
				if err != nil {
					return err
				}

				convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(r.TargetDBCharset)])
				if err != nil {
					return fmt.Errorf("table column name rule [%s] charset convert failed, %v", columnName, err)
				}
				columnName = string(convTargetRaw)

				// 优先级
				// column > table > schema > buildin
				if len(columnDataTypeMapSlice) == 0 {
					columnDatatypeMap[columnName] = LoadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)
				}

				// only column rule
				columnTypeFromColumn := loadColumnTypeRuleOnlyUsingColumn(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice)

				// table or schema rule check, return column type
				columnTypeFromOther := LoadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

				// column or other rule check, return column type
				switch {
				case columnTypeFromColumn != buildInColumnType && columnTypeFromOther == buildInColumnType:
					columnDatatypeMap[columnName] = common.StringUPPER(columnTypeFromColumn)
				case columnTypeFromColumn != buildInColumnType && columnTypeFromOther != buildInColumnType:
					columnDatatypeMap[columnName] = common.StringUPPER(columnTypeFromColumn)
				case columnTypeFromColumn == buildInColumnType && columnTypeFromOther != buildInColumnType:
					columnDatatypeMap[columnName] = common.StringUPPER(columnTypeFromOther)
				default:
					columnDatatypeMap[columnName] = common.StringUPPER(buildInColumnType)
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
	return tableColumnDatatypeMap, nil
}

func (r *Change) ChangeTableColumnDefaultValue() (map[string]map[string]bool, map[string]map[string]string, error) {
	startTime := time.Now()
	tableDefaultValSource := make(map[string]map[string]bool)
	tableDefaultValMap := make(map[string]map[string]string)
	// 获取内置字段默认值映射规则 -> global
	globalDefaultValueMapSlice, err := meta.NewBuildinGlobalDefaultvalModel(r.MetaDB).DetailGlobalDefaultVal(r.Ctx, &meta.BuildinGlobalDefaultval{
		DBTypeS: r.DBTypeS,
		DBTypeT: r.DBTypeT,
	})
	if err != nil {
		return tableDefaultValSource, tableDefaultValMap, err
	}

	// 获取自定义字段默认值映射规则 -> column
	columnDefaultValueMapSlice, err := meta.NewBuildinColumnDefaultvalModel(r.MetaDB).DetailColumnDefaultVal(r.Ctx, &meta.BuildinColumnDefaultval{
		DBTypeS:     r.DBTypeS,
		DBTypeT:     r.DBTypeT,
		SchemaNameS: r.SourceSchemaName,
	})
	if err != nil {
		return tableDefaultValSource, tableDefaultValMap, err
	}

	reg, err := regexp.Compile(`^.+\(\)$`)
	if err != nil {
		return tableDefaultValSource, tableDefaultValMap, err
	}

	wg := &errgroup.Group{}
	wg.SetLimit(r.Threads)

	columnDefaultChan := make(chan map[string]map[string]string, common.ChannelBufferSize)
	columnDefaultSourceChan := make(chan map[string]map[string]bool, common.ChannelBufferSize)

	doneC := make(chan struct{})
	doneS := make(chan struct{})

	go func(done func()) {
		for c := range columnDefaultChan {
			for key, val := range c {
				tableDefaultValMap[key] = val
			}
		}
		done()
	}(func() {
		doneC <- struct{}{}
	})

	go func(done func()) {
		for c := range columnDefaultSourceChan {
			for key, val := range c {
				tableDefaultValSource[key] = val
			}
		}
		done()
	}(func() {
		doneS <- struct{}{}
	})

	for _, table := range r.SourceTables {
		sourceTable := table
		wg.Go(func() error {
			// 获取表字段信息
			tableColumnINFO, err := r.MySQL.GetMySQLTableColumn(r.SourceSchemaName, sourceTable)
			if err != nil {
				return err
			}

			var (
				fromDB      bool
				dataDefault string
			)

			columnDataDefaultValSource := make(map[string]bool, 1)
			columnDataDefaultValMap := make(map[string]string, 1)
			tableDefaultValTempMap := make(map[string]map[string]string, 1)
			tableDefaultValSourceTempMap := make(map[string]map[string]bool, 1)

			for _, rowCol := range tableColumnINFO {
				// Special M2O
				// MySQL/TiDB default value character insensitive
				var regDataDefault string

				if common.IsContainString(common.SpecialMySQLDataDefaultsWithDataTYPE, common.StringUPPER(rowCol["DATA_TYPE"])) {
					if reg.MatchString(rowCol["DATA_DEFAULT"]) || strings.EqualFold(rowCol["DATA_DEFAULT"], "CURRENT_TIMESTAMP") {
						regDataDefault = rowCol["DATA_DEFAULT"]
					} else {
						// 修复默认值类似 'PC' 数据单引号问题
						// oracle 单引号默认值 '''PC''' , mysql 单引号默认值 'PC'，对于字符数据非默认值带单引号的是 PC
						// mysql 前后再增加两个单引号，输出 '''PC'''
						if strings.HasPrefix(rowCol["DATA_DEFAULT"], "'") && strings.HasSuffix(rowCol["DATA_DEFAULT"], "'") {
							regDataDefault = fmt.Sprintf("''%s", rowCol["DATA_DEFAULT"])
							regDataDefault = fmt.Sprintf("%s''", regDataDefault)
							// 特殊数据处理
						} else if strings.EqualFold(rowCol["DATA_DEFAULT"], common.OracleNULLSTRINGTableAttrWithoutNULL) {
							regDataDefault = "NULL"
						} else if strings.EqualFold(rowCol["DATA_DEFAULT"], common.OracleNULLSTRINGTableAttrWithCustom) {
							regDataDefault = "''"
						} else {
							regDataDefault = common.StringsBuilder(`'`, rowCol["DATA_DEFAULT"], `'`)
						}
					}
				} else {
					if strings.EqualFold(rowCol["DATA_DEFAULT"], common.OracleNULLSTRINGTableAttrWithoutNULL) {
						regDataDefault = "NULL"
					} else if strings.EqualFold(rowCol["DATA_DEFAULT"], common.OracleNULLSTRINGTableAttrWithCustom) {
						regDataDefault = "''"
					} else {
						regDataDefault = rowCol["DATA_DEFAULT"]
					}
				}

				// 优先级
				// column > global
				columnName := rowCol["COLUMN_NAME"]
				convUtf8Raw, err := common.CharsetConvert([]byte(columnName), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(r.SourceDBCharset)], common.CharsetUTF8MB4)
				if err != nil {
					return fmt.Errorf("table column default value rule [%s] charset convert failed, %v", columnName, err)
				}

				convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(r.TargetDBCharset)])
				if err != nil {
					return fmt.Errorf("table column default value rule [%s] charset convert failed, %v", columnName, err)
				}
				columnName = string(convTargetRaw)
				fromDB, dataDefault = LoadColumnDefaultValueRule(columnName, regDataDefault, columnDefaultValueMapSlice, globalDefaultValueMapSlice)

				columnDataDefaultValSource[columnName] = fromDB
				columnDataDefaultValMap[columnName] = dataDefault
			}

			tableDefaultValTempMap[sourceTable] = columnDataDefaultValMap
			tableDefaultValSourceTempMap[sourceTable] = columnDataDefaultValSource

			columnDefaultChan <- tableDefaultValTempMap
			columnDefaultSourceChan <- tableDefaultValSourceTempMap
			return nil
		})
	}

	if err = wg.Wait(); err != nil {
		return nil, nil, err
	}

	close(columnDefaultChan)
	<-doneC

	close(columnDefaultSourceChan)
	<-doneS

	zap.L().Warn("get source table column default value mapping rules",
		zap.String("schema", r.SourceSchemaName),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return tableDefaultValSource, tableDefaultValMap, nil
}
