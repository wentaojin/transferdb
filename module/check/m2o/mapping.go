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
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	reverseM2O "github.com/wentaojin/transferdb/module/reverse/m2o"
	"regexp"
	"strings"
)

/*
MySQL 表字段映射转换 -> Check 阶段
*/
func GenOracleTableColumnMeta(ctx context.Context, metaDB *meta.Meta, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName, oracleDBVersion string, columnINFO Column, oracleExtendedMode bool) (string, error) {
	var (
		columnCollation string
		nullable        string
		dataDefault     string
		columnType      string
		columnMeta      string
		err             error
	)

	reg, err := regexp.Compile(`^.+\(\)$`)
	if err != nil {
		return "", err
	}

	// Special M2O
	// MySQL/TiDB default value character insensitive
	var regDataDefault string

	if common.IsContainString(common.SpecialMySQLDataDefaultsWithDataTYPE, common.StringUPPER(columnINFO.DataType)) {
		if reg.MatchString(columnINFO.DataDefault) || strings.EqualFold(columnINFO.DataDefault, "CURRENT_TIMESTAMP") || strings.EqualFold(columnINFO.DataDefault, "") {
			regDataDefault = columnINFO.DataDefault
		} else {
			regDataDefault = common.StringsBuilder(`'`, columnINFO.DataDefault, `'`)
		}
	} else {
		regDataDefault = columnINFO.DataDefault
	}

	dataDefault, err = ChangeTableColumnDefaultValue(ctx, metaDB, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName, regDataDefault)
	if err != nil {
		return columnMeta, err
	}

	columnType, err = ChangeTableColumnType(ctx, metaDB, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName, columnINFO)
	if err != nil {
		return "", err
	}

	// 字段排序规则检查
	if columnCollationMapVal, ok := common.MySQLDBCollationMap[strings.ToLower(columnINFO.Collation)]; ok {
		if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
			// oracle 12.2 版本及以上，字符集 columnCollation 开启需激活 extended 特性
			// columnCollation BINARY_CS : Both case and accent sensitive. This is default if no extension is used.
			switch {
			case strings.EqualFold(columnINFO.Collation, "utf8mb4_bin") || strings.EqualFold(columnINFO.Collation, "utf8_bin"):
				columnCollationArr := strings.Split(columnCollationMapVal, "/")
				if oracleExtendedMode {
					columnCollation = columnCollationArr[0]
				} else {
					// ORACLE 12.2 版本及以上非 extended 模式不支持设置字段 columnCollation
					// columncolumnCollation = columnCollationArr[1]
					columnCollation = ""
				}
			default:
				columnCollation = columnCollationMapVal
			}

		} else {
			// ORACLE 12.2 以下版本没有字段级别 columnCollation，使用 oracledb 实例级别 columnCollation
			columnCollation = ""
		}
	} else {
		switch {
		case !strings.EqualFold(columnINFO.Collation, "UNKNOWN"):
			return columnMeta, fmt.Errorf(`table json [%v], error on generate mysql table column column collation [%v]`, columnINFO.String(), columnINFO.Collation)
		case strings.EqualFold(columnINFO.Collation, "UNKNOWN"):
			// column columnCollation value UNKNOWN, 代表是非字符串数据类型
			// skip ignore
			columnCollation = ""
		default:
			return columnMeta, fmt.Errorf("mysql table column meta generate failed, column [%s] not support column collation [%s]", columnName, columnINFO.Collation)
		}
	}

	if common.IsContainString(common.SpecialMySQLColumnCollationWithDataTYPE, common.StringUPPER(columnINFO.DataType)) {
		// M2O reverse mysql table to oracle table  special data type configure columnCollation error
		// ORA-43912: invalid columnCollation specified for a CLOB or NCLOB value
		// columncolumnCollation = ""
		columnCollation = ""
	}

	if strings.EqualFold(columnINFO.NULLABLE, "Y") {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

	if strings.EqualFold(nullable, "NULL") {
		// M2O
		switch {
		case columnCollation != "" && dataDefault != "":
			columnMeta = fmt.Sprintf("%s %s COLLATE %s DEFAULT %s", columnName, columnType, columnCollation, dataDefault)
		case columnCollation != "" && dataDefault == "":
			columnMeta = fmt.Sprintf("%s %s COLLATE %s", columnName, columnType, columnCollation)
		case columnCollation == "" && dataDefault != "":
			columnMeta = fmt.Sprintf("%s %s DEFAULT %s", columnName, columnType, dataDefault)
		case columnCollation == "" && dataDefault == "":
			columnMeta = fmt.Sprintf("%s %s", columnName, columnType)
		default:
			return columnMeta, fmt.Errorf("error on gen mysql schema table column meta with nullable, rule: %v", columnINFO.String())
		}
	} else {
		// M2O
		switch {
		case columnCollation != "" && dataDefault != "":
			columnMeta = fmt.Sprintf("%s %s COLLATE %s DEFAULT %s %s", columnName, columnType, columnCollation, dataDefault, nullable)
		case columnCollation != "" && dataDefault == "":
			columnMeta = fmt.Sprintf("%s %s COLLATE %s %s", columnName, columnType, columnCollation, nullable)
		case columnCollation == "" && dataDefault != "":
			columnMeta = fmt.Sprintf("%s %s DEFAULT %s %s", columnName, columnType, dataDefault, nullable)
		case columnCollation == "" && dataDefault == "":
			columnMeta = fmt.Sprintf("%s %s %s", columnName, columnType, nullable)
		default:
			return columnMeta, fmt.Errorf("error on gen mysql schema table column meta without nullable, rule: %v", columnINFO.String())
		}
	}

	return columnMeta, nil
}

// 数据库查询获取自定义表结构转换规则
// 加载数据类型转换规则【处理字段级别、表级别、库级别数据类型映射规则】
// 数据类型转换规则判断，未设置自定义规则，默认采用内置默认字段类型转换
func ChangeTableColumnType(ctx context.Context, metaDB *meta.Meta, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName string, columnINFO Column) (string, error) {
	var columnType string
	// 获取内置映射规则
	buildinDatatypeNames, err := meta.NewBuildinDatatypeRuleModel(metaDB).BatchQueryBuildinDatatype(ctx, &meta.BuildinDatatypeRule{
		DBTypeS: dbTypeS,
		DBTypeT: dbTypeT,
	})
	if err != nil {
		return columnType, err
	}
	originColumnType, buildInColumnType, err := reverseM2O.MySQLTableColumnMapRule(sourceSchema, sourceTableName, reverseM2O.Column{
		DataType:                columnINFO.DataType,
		CharLength:              columnINFO.CharLength,
		CharUsed:                columnINFO.CharUsed,
		CharacterSet:            columnINFO.CharacterSet,
		Collation:               columnINFO.Collation,
		OracleOriginDataDefault: columnINFO.OracleOriginDataDefault,
		MySQLOriginDataDefault:  columnINFO.MySQLOriginDataDefault,
		ColumnInfo: reverseM2O.ColumnInfo{
			DataLength:        columnINFO.DataLength,
			DataPrecision:     columnINFO.DataPrecision,
			DataScale:         columnINFO.DataScale,
			DatetimePrecision: columnINFO.DatetimePrecision,
			NULLABLE:          columnINFO.NULLABLE,
			DataDefault:       columnINFO.DataDefault,
			Comment:           columnINFO.Comment,
		},
	}, buildinDatatypeNames)
	if err != nil {
		return columnType, err
	}
	// 获取自定义映射规则
	columnDataTypeMapSlice, err := meta.NewColumnDatatypeRuleModel(metaDB).DetailColumnRule(ctx, &meta.ColumnDatatypeRule{
		DBTypeS:     dbTypeS,
		DBTypeT:     dbTypeT,
		SchemaNameS: sourceSchema,
		TableNameS:  sourceTableName,
		ColumnNameS: columnName,
	})
	if err != nil {
		return columnType, err
	}

	tableDataTypeMapSlice, err := meta.NewTableDatatypeRuleModel(metaDB).DetailTableRule(ctx, &meta.TableDatatypeRule{
		DBTypeS:     dbTypeS,
		DBTypeT:     dbTypeT,
		SchemaNameS: sourceSchema,
		TableNameS:  sourceTableName,
	})
	if err != nil {
		return columnType, err
	}

	schemaDataTypeMapSlice, err := meta.NewSchemaDatatypeRuleModel(metaDB).DetailSchemaRule(ctx, &meta.SchemaDatatypeRule{
		DBTypeS:     dbTypeS,
		DBTypeT:     dbTypeT,
		SchemaNameS: sourceSchema,
	})
	if err != nil {
		return columnType, err
	}

	// 优先级
	// column > table > schema > buildin
	if len(columnDataTypeMapSlice) == 0 {
		return loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType,
			tableDataTypeMapSlice, schemaDataTypeMapSlice), nil
	}

	// only column rule
	columnTypeFromColumn := loadColumnTypeRuleOnlyUsingColumn(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice)

	// table or schema rule check, return column type
	columnTypeFromOther := loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	// column or other rule check, return column type
	switch {
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther == buildInColumnType:
		return strings.ToUpper(columnTypeFromColumn), nil
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther != buildInColumnType:
		return strings.ToUpper(columnTypeFromColumn), nil
	case columnTypeFromColumn == buildInColumnType && columnTypeFromOther != buildInColumnType:
		return strings.ToUpper(columnTypeFromOther), nil
	default:
		return strings.ToUpper(buildInColumnType), nil
	}
}

func ChangeTableColumnDefaultValue(ctx context.Context, metaDB *meta.Meta, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName, dataDefault string) (string, error) {
	columnDefaultValueMapSlice, err := meta.NewBuildinColumnDefaultvalModel(metaDB).DetailColumnDefaultVal(ctx, &meta.BuildinColumnDefaultval{
		DBTypeS:     dbTypeS,
		DBTypeT:     dbTypeT,
		SchemaNameS: sourceSchema,
		TableNameS:  sourceTableName,
		ColumnNameS: columnName,
	})
	if err != nil {
		return dataDefault, err
	}

	globalDefaultValueMapSlice, err := meta.NewBuildinGlobalDefaultvalModel(metaDB).DetailGlobalDefaultVal(ctx, &meta.BuildinGlobalDefaultval{
		DBTypeS: dbTypeS,
		DBTypeT: dbTypeT,
	})
	if err != nil {
		return dataDefault, err
	}

	return loadColumnDefaultValueRule(columnName, dataDefault, columnDefaultValueMapSlice, globalDefaultValueMapSlice), nil
}

func loadColumnDefaultValueRule(columnName string, defaultValue string, columnDefaultValueMapSlice []meta.BuildinColumnDefaultval, globalDefaultValueMapSlice []meta.BuildinGlobalDefaultval) string {
	if len(columnDefaultValueMapSlice) == 0 && len(globalDefaultValueMapSlice) == 0 {
		return defaultValue
	}

	// 默认值优先级: 字段级别默认值 > 全局级别默认值
	if len(columnDefaultValueMapSlice) > 0 {
		for _, dv := range columnDefaultValueMapSlice {
			if strings.EqualFold(columnName, dv.ColumnNameS) && strings.EqualFold(strings.TrimSpace(dv.DefaultValueS), strings.TrimSpace(defaultValue)) {
				return dv.DefaultValueT
			}
		}
	}

	for _, dv := range globalDefaultValueMapSlice {
		if strings.EqualFold(strings.TrimSpace(dv.DefaultValueS), strings.TrimSpace(defaultValue)) && dv.DefaultValueT != "" {
			return dv.DefaultValueT
		}
	}
	return defaultValue
}

func loadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableDatatypeRule, schemaDataTypeMapSlice []meta.SchemaDatatypeRule) string {
	switch {
	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) == 0:
		return loadColumnTypeRuleOnlyUsingTable(originColumnType, buildInColumnType, tableDataTypeMapSlice)

	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) != 0:
		return loadDataTypeRuleUsingTableAndSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) != 0:
		return loadColumnTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) == 0:
		return strings.ToUpper(buildInColumnType)
	default:
		panic(fmt.Errorf("mysql data type mapping failed, tableDataTypeMapSlice [%v],schemaDataTypeMapSlice [%v]", len(tableDataTypeMapSlice), len(schemaDataTypeMapSlice)))
	}
}

func loadDataTypeRuleUsingTableAndSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableDatatypeRule, schemaDataTypeMapSlice []meta.SchemaDatatypeRule) string {
	// 规则判断
	customTableDataType := loadColumnTypeRuleOnlyUsingTable(originColumnType, buildInColumnType, tableDataTypeMapSlice)

	customSchemaDataType := loadColumnTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType, schemaDataTypeMapSlice)

	switch {
	case customTableDataType == buildInColumnType && customSchemaDataType != buildInColumnType:
		return customSchemaDataType
	case customTableDataType != buildInColumnType && customSchemaDataType == buildInColumnType:
		return customTableDataType
	case customTableDataType != buildInColumnType && customSchemaDataType != buildInColumnType:
		return customTableDataType
	default:
		return strings.ToUpper(buildInColumnType)
	}
}

/*
	库、表、字段自定义映射规则
*/
// 表级别自定义映射规则
func loadColumnTypeRuleOnlyUsingTable(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableDatatypeRule) string {
	if len(tableDataTypeMapSlice) == 0 {
		return buildInColumnType
	}
	for _, tbl := range tableDataTypeMapSlice {
		if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "YEAR") && tbl.ColumnTypeT != "" {
			return strings.ToUpper(tbl.ColumnTypeT)
		}
		if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "REAL") && tbl.ColumnTypeT != "" {
			return strings.ToUpper(tbl.ColumnTypeT)
		}
		if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
			return strings.ToUpper(tbl.ColumnTypeT)
		}
	}
	return strings.ToUpper(buildInColumnType)
}

// 库级别自定义映射规则
func loadColumnTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType string, schemaDataTypeMapSlice []meta.SchemaDatatypeRule) string {
	if len(schemaDataTypeMapSlice) == 0 {
		return buildInColumnType
	}

	for _, tbl := range schemaDataTypeMapSlice {
		if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "YEAR") && tbl.ColumnTypeT != "" {
			return strings.ToUpper(tbl.ColumnTypeT)
		}
		if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "REAL") && tbl.ColumnTypeT != "" {
			return strings.ToUpper(tbl.ColumnTypeT)
		}
		if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
			return strings.ToUpper(tbl.ColumnTypeT)
		}
	}
	return strings.ToUpper(buildInColumnType)
}

// 字段级别自定义映射规则
func loadColumnTypeRuleOnlyUsingColumn(columnName string, originColumnType string, buildInColumnType string, columnDataTypeMapSlice []meta.ColumnDatatypeRule) string {
	if len(columnDataTypeMapSlice) == 0 {
		return buildInColumnType
	}
	for _, tbl := range columnDataTypeMapSlice {
		if strings.EqualFold(tbl.ColumnNameS, columnName) {
			if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "YEAR") && tbl.ColumnTypeT != "" {
				return strings.ToUpper(tbl.ColumnTypeT)
			}
			if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "REAL") && tbl.ColumnTypeT != "" {
				return strings.ToUpper(tbl.ColumnTypeT)
			}
			if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
				return strings.ToUpper(tbl.ColumnTypeT)
			}
		}
	}
	return strings.ToUpper(buildInColumnType)
}
