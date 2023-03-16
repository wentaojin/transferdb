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
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	reverseO2M "github.com/wentaojin/transferdb/module/reverse/o2m"
	"strings"
)

/*
Oracle 表字段映射转换 -> Check 阶段
*/
func GenOracleTableColumnMeta(ctx context.Context, metaDB *meta.Meta, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName string, columnINFO Column) (string, error) {
	var (
		nullable        string
		dataDefault     string
		comment         string
		columnCharacter string
		columnCollation string
		err             error

		columnMeta string
	)

	if strings.EqualFold(columnINFO.NULLABLE, "Y") {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

	if !strings.EqualFold(columnINFO.Comment, "") {
		comment = common.SpecialLettersUsingMySQL([]byte(columnINFO.Comment))
	} else {
		comment = columnINFO.Comment
	}

	// 字段字符集以及排序规则
	// 两者同时间存在否则异常
	if common.IsContainString([]string{"NUMBER",
		"DATE",
		"DECIMAL",
		"DEC",
		"DOUBLE PRECISION",
		"FLOAT",
		"INTEGER",
		"INT",
		"BINARY_FLOAT",
		"BINARY_DOUBLE",
		"NUMERIC",
		"REAL",
		"SMALLINT"}, columnINFO.DataType) || strings.Contains(columnINFO.DataType, "TIMESTAMP") {
		columnCharacter = ""
		columnCollation = ""
	} else {
		columnCharacter = columnINFO.CharacterSet
		columnCollation = columnINFO.Collation
	}
	if (columnCharacter == "" && columnCollation != "") || (columnCharacter != "" && columnCollation == "") {
		return columnMeta, fmt.Errorf(`oracle table column meta generate failed, column [%v] json: [%v]`, columnName, columnINFO.String())
	}

	dataDefault, err = ChangeTableColumnDefaultValue(ctx, metaDB, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName, columnINFO.DataDefault)
	if err != nil {
		return columnMeta, err
	}

	columnType, err := ChangeTableColumnType(ctx, metaDB, dbTypeS, dbTypeT, sourceSchema, sourceTableName, columnName, columnINFO)
	if err != nil {
		return "", err
	}

	switch {
	case nullable == "NULL" && columnCharacter == "" && columnCollation == "":
		switch {
		case comment != "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", columnName, columnType, dataDefault, comment)
		case comment == "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault)
		case comment != "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s COMMENT %s", columnName, columnType, comment)
		case comment == "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s", columnName, columnType)
		default:
			return columnMeta, fmt.Errorf(`oracle table column meta generate failed, column [%v] column meta [%v] by first`, columnName, columnMeta)
		}
	case nullable == "NULL" && columnCharacter != "" && columnCollation != "":
		switch {
		case comment != "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s DEFAULT %s COMMENT %s", columnName, columnType, columnCharacter, columnCollation, dataDefault, comment)
		case comment == "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s DEFAULT %s", columnName, columnType, columnCharacter, columnCollation, dataDefault)
		case comment != "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s COMMENT %s", columnName, columnType,
				columnCharacter, columnCollation, comment)
		case comment == "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s", columnName, columnType, columnCharacter, columnCollation)
		default:
			return columnMeta, fmt.Errorf(`oracle table column meta generate failed, column [%v] column meta [%v] by second`, columnName, columnMeta)
		}
	case nullable != "NULL" && columnCharacter == "" && columnCollation == "":
		switch {
		case comment != "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", columnName, columnType, nullable, dataDefault, comment)
		case comment != "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s %s COMMENT %s", columnName, columnType, nullable, comment)
		case comment == "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault)
		case comment == "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable)
		default:
			return columnMeta, fmt.Errorf(`oracle table column meta generate failed, column [%v] column meta [%v] by third`, columnName, columnMeta)
		}
	case nullable != "NULL" && columnCharacter != "" && columnCollation != "":
		switch {
		case comment != "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s %s DEFAULT %s COMMENT %s", columnName, columnType, columnCharacter, columnCollation, nullable, dataDefault, comment)
		case comment != "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s %s COMMENT %s", columnName, columnType, columnCharacter, columnCollation, nullable, comment)
		case comment == "" && dataDefault != "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s %s DEFAULT %s", columnName, columnType, columnCharacter, columnCollation, nullable, dataDefault)
		case comment == "" && dataDefault == "":
			columnMeta = fmt.Sprintf("`%s` %s CHARACTER SET %s COLLATE %s %s", columnName, columnType, columnCharacter, columnCollation, nullable)
		default:
			return columnMeta, fmt.Errorf(`oracle table column meta generate failed, column [%v] column meta [%v] by four`, columnName, columnMeta)
		}
	default:
		return columnMeta, fmt.Errorf(`oracle table column meta generate failed, column [%v] nullable [%s] character set [%s] collation [%v] by five`, columnName, nullable, columnCharacter, columnCollation)
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
	originColumnType, buildInColumnType, err := reverseO2M.OracleTableColumnMapRule(sourceSchema, sourceTableName, reverseO2M.Column{
		DataType:                columnINFO.DataType,
		CharLength:              columnINFO.CharLength,
		CharUsed:                columnINFO.CharUsed,
		CharacterSet:            columnINFO.CharacterSet,
		Collation:               columnINFO.Collation,
		OracleOriginDataDefault: columnINFO.OracleOriginDataDefault,
		MySQLOriginDataDefault:  columnINFO.MySQLOriginDataDefault,
		ColumnInfo: reverseO2M.ColumnInfo{
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
	// 处理 oracle 默认值 ('xxx') 或者 (xxx)
	if strings.HasPrefix(dataDefault, "(") && strings.HasSuffix(dataDefault, ")") {
		dataDefault = strings.TrimLeft(dataDefault, "(")
		dataDefault = strings.TrimRight(dataDefault, ")")
	}

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

func loadColumnDefaultValueRule(columnName, defaultValue string, columnDefaultValueMapSlice []meta.BuildinColumnDefaultval, globalDefaultValueMapSlice []meta.BuildinGlobalDefaultval) string {
	if len(columnDefaultValueMapSlice) == 0 && len(globalDefaultValueMapSlice) == 0 {
		return defaultValue
	}

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

func loadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableDatatypeRule,
	schemaDataTypeMapSlice []meta.SchemaDatatypeRule) string {
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
		panic(fmt.Errorf("oracle data type mapping failed, tableDataTypeMapSlice [%v],schemaDataTypeMapSlice [%v]", len(tableDataTypeMapSlice), len(schemaDataTypeMapSlice)))
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
		/*
			number 类型处理：函数匹配 ->  GetOracleTableColumn
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number(38,127)
			- number -> number(38,127)
			- number(5) -> number(5)
			- number(8,9) -> number(8,9)
		*/
		if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "NUMBER") {
			switch {
			case strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "*") && strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ","):
				if strings.EqualFold(strings.Replace(tbl.ColumnTypeS, "*", "38", -1), originColumnType) &&
					tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			case strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "*") && !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ","):
				if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
					tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			case !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "(") && !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ")"):
				if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
					tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			default:
				if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			}
		} else {
			if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
				return strings.ToUpper(tbl.ColumnTypeT)
			}
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
		/*
			number 类型处理：函数匹配 ->  GetOracleTableColumn
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number(38,127)
			- number -> number(38,127)
			- number(5) -> number(5)
			- number(8,9) -> number(8,9)
		*/
		if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "NUMBER") {
			switch {
			case strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "*") && strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ","):
				if strings.EqualFold(strings.Replace(tbl.ColumnTypeS, "*", "38", -1), originColumnType) &&
					tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			case strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "*") && !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ","):
				if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
					tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			case !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "(") && !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ")"):
				if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
					tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			default:
				if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			}
		} else {
			if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
				return strings.ToUpper(tbl.ColumnTypeT)
			}
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
			/*
				number 类型处理：函数匹配 ->  GetOracleTableColumn
				- number(*,10) -> number(38,10)
				- number(*,0) -> number(38,0)
				- number(*) -> number(38,127)
				- number -> number(38,127)
				- number(5) -> number(5)
				- number(8,9) -> number(8,9)
			*/
			if strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "NUMBER") {
				switch {
				case strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "*") && strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ","):
					if strings.EqualFold(strings.Replace(tbl.ColumnTypeS, "*", "38", -1), originColumnType) &&
						tbl.ColumnTypeT != "" {
						return strings.ToUpper(tbl.ColumnTypeT)
					}
				case strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "*") && !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ","):
					if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
						tbl.ColumnTypeT != "" {
						return strings.ToUpper(tbl.ColumnTypeT)
					}
				case !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), "(") && !strings.Contains(strings.ToUpper(tbl.ColumnTypeS), ")"):
					if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
						tbl.ColumnTypeT != "" {
						return strings.ToUpper(tbl.ColumnTypeT)
					}
				default:
					if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
						return strings.ToUpper(tbl.ColumnTypeT)
					}
				}
			} else {
				if strings.EqualFold(tbl.ColumnTypeS, originColumnType) && tbl.ColumnTypeT != "" {
					return strings.ToUpper(tbl.ColumnTypeT)
				}
			}
		}
	}
	return strings.ToUpper(buildInColumnType)
}
