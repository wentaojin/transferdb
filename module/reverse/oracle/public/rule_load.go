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
	"fmt"
	"github.com/wentaojin/transferdb/database/meta"
	"strings"
)

func LoadColumnDefaultValueRule(columnName, defaultValue string, defaultValueColumnMapSlice []meta.BuildinColumnDefaultval, defaultValueGlobalMapSlice []meta.BuildinGlobalDefaultval) string {
	// 额外处理 Oracle 默认值 ('6') 或者 (5) 或者 ('xsddd') 等包含小括号的默认值，而非 '(xxxx)' 之类的默认值
	// Oracle 对于同类型 ('xxx') 或者 (xxx) 内部会自动处理，所以 O2M/O2T 需要处理成 'xxx' 或者 xxx
	if strings.HasPrefix(defaultValue, "(") && strings.HasSuffix(defaultValue, ")") {
		defaultValue = strings.TrimLeft(defaultValue, "(")
		defaultValue = strings.TrimRight(defaultValue, ")")
	}

	if len(defaultValueColumnMapSlice) == 0 && len(defaultValueGlobalMapSlice) == 0 {
		return defaultValue
	}

	// 默认值优先级: 字段级别默认值 > 全局级别默认值
	if len(defaultValueColumnMapSlice) > 0 {
		for _, dv := range defaultValueColumnMapSlice {
			if strings.EqualFold(columnName, dv.ColumnNameS) && strings.EqualFold(strings.TrimSpace(dv.DefaultValueS), strings.TrimSpace(defaultValue)) {
				return dv.DefaultValueT
			}
		}
	}

	for _, dv := range defaultValueGlobalMapSlice {
		if strings.EqualFold(strings.TrimSpace(dv.DefaultValueS), strings.TrimSpace(defaultValue)) && dv.DefaultValueT != "" {
			return dv.DefaultValueT
		}
	}
	return defaultValue
}

func LoadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableDatatypeRule,
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
func LoadColumnTypeRuleOnlyUsingColumn(columnName string, originColumnType string, buildInColumnType string, columnDataTypeMapSlice []meta.ColumnDatatypeRule) string {
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
