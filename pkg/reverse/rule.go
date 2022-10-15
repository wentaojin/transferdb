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
package reverse

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
)

func ChangeOracleTableName(sourceTableName string, targetTableName string) string {
	if targetTableName == "" {
		return sourceTableName
	}
	if targetTableName != "" {
		return targetTableName
	}
	return sourceTableName
}

// 数据库查询获取自定义表结构转换规则
// 加载数据类型转换规则【处理字段级别、表级别、库级别数据类型映射规则】
// 数据类型转换规则判断，未设置自定义规则，默认采用内置默认字段类型转换
func ChangeTableColumnType(columnName string,
	originColumnType string,
	buildInColumnType string,
	columnDataTypeMapSlice []service.ColumnRuleMap,
	tableDataTypeMapSlice []service.TableRuleMap,
	schemaDataTypeMapSlice []service.SchemaRuleMap) string {

	if len(columnDataTypeMapSlice) == 0 {
		return loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)
	}

	columnTypeFromColumn := loadDataTypeRuleOnlyUsingColumn(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice)
	columnTypeFromOther := loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	switch {
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther == buildInColumnType:
		return strings.ToUpper(columnTypeFromColumn)
	case columnTypeFromColumn != buildInColumnType && columnTypeFromOther != buildInColumnType:
		return strings.ToUpper(columnTypeFromColumn)
	case columnTypeFromColumn == buildInColumnType && columnTypeFromOther != buildInColumnType:
		return strings.ToUpper(columnTypeFromOther)
	default:
		return strings.ToUpper(buildInColumnType)
	}
}

func GenTableColumnMeta(reverseMode, columnName, columnType, dataNullable, comments, defaultValue, columnCollation string,
	defaultValueMapSlice []service.DefaultValueMap) string {
	var (
		nullable    string
		colMeta     string
		dataDefault string
		comment     string
		collation   string
	)

	if dataNullable == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

	if comments != "" {
		comments = strings.Replace(comments, "\"", "'", -1)

		match, _ := regexp.MatchString("'(.*)'", comments)
		if match {
			comment = fmt.Sprintf("\"%s\"", comments)
		} else {
			comment = fmt.Sprintf("'%s'", comments)
		}
	}

	if columnCollation != "" {
		collation = columnCollation
	}

	if defaultValue != "" {
		dataDefault = LoadDataDefaultValueRule(defaultValue, defaultValueMapSlice)
	} else {
		dataDefault = defaultValue
	}

	if nullable == "NULL" {
		// O2M
		if strings.EqualFold(reverseMode, utils.ReverseModeO2M) {
			switch {
			case collation != "" && comment != "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s COMMENT %s", columnName, columnType, collation, dataDefault, comment)
			case collation != "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s", columnName, columnType, collation, dataDefault)
			case collation != "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s", columnName, columnType, collation)
			case collation != "" && comment != "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s COMMENT %s", columnName, columnType, collation, comment)
			case collation == "" && comment != "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", columnName, columnType, dataDefault, comment)
			case collation == "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault)
			case collation == "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s", columnName, columnType)
			case collation == "" && comment != "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s COMMENT %s", columnName, columnType, comment)
			default:
				panic(fmt.Errorf(`oracle table column meta generate failed, 
	column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`,
					columnName, columnType, collation, comment, defaultValue, nullable))
			}
		}

		// M2O
		if strings.EqualFold(reverseMode, utils.ReverseModeM2O) {
			switch {
			case collation != "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("%s %s COLLATE %s DEFAULT %s", columnName, columnType, collation, dataDefault)
			case collation != "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("%s %s COLLATE %s", columnName, columnType, collation)
			case collation == "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("%s %s DEFAULT %s", columnName, columnType, dataDefault)
			case collation == "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("%s %s", columnName, columnType)
			default:
				panic(fmt.Errorf(`oracle table column meta generate failed, 
	column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`,
					columnName, columnType, collation, comment, defaultValue, nullable))
			}
		}
	} else {
		//O2M
		if strings.EqualFold(reverseMode, utils.ReverseModeO2M) {
			switch {
			case collation != "" && comment != "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s COMMENT %s",
					columnName, columnType, collation, nullable, dataDefault, comment)
			case collation != "" && comment != "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s COMMENT %s", columnName, columnType, collation, nullable, comment)
			case collation != "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s", columnName, columnType, collation, nullable, dataDefault)
			case collation != "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s COLLATE %s %s", columnName, columnType, collation, nullable)
			case collation == "" && comment != "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", columnName, columnType, nullable, dataDefault, comment)
			case collation == "" && comment != "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s %s COMMENT %s", columnName, columnType, nullable, comment)
			case collation == "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault)
			case collation == "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable)
			default:
				panic(fmt.Errorf(`oracle table column meta generate failed, 
	column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`,
					columnName, columnType, collation, comment, defaultValue, nullable))
			}
		}

		// M2O
		if strings.EqualFold(reverseMode, utils.ReverseModeM2O) {
			switch {
			case collation != "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("%s %s COLLATE %s DEFAULT %s %s", columnName, columnType, collation, dataDefault, nullable)
			case collation != "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("%s %s COLLATE %s %s", columnName, columnType, collation, nullable)
			case collation == "" && comment == "" && dataDefault != "":
				colMeta = fmt.Sprintf("%s %s DEFAULT %s %s", columnName, columnType, dataDefault, nullable)
			case collation == "" && comment == "" && dataDefault == "":
				colMeta = fmt.Sprintf("%s %s %s", columnName, columnType, nullable)
			default:
				panic(fmt.Errorf(`oracle table column meta generate failed, 
	column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`,
					columnName, columnType, collation, comment, defaultValue, nullable))
			}
		}

	}
	return colMeta
}

func LoadDataDefaultValueRule(defaultValue string, defaultValueMapSlice []service.DefaultValueMap) string {
	if len(defaultValueMapSlice) == 0 {
		return defaultValue
	}

	for _, dv := range defaultValueMapSlice {
		if strings.EqualFold(strings.TrimSpace(dv.SourceDefaultValue), strings.TrimSpace(defaultValue)) && dv.TargetDefaultValue != "" {
			return dv.TargetDefaultValue
		}
	}
	return defaultValue
}

func loadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableRuleMap,
	schemaDataTypeMapSlice []service.SchemaRuleMap) string {
	switch {
	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) == 0:
		return loadDataTypeRuleOnlyUsingTable(originColumnType, buildInColumnType, tableDataTypeMapSlice)

	case len(tableDataTypeMapSlice) != 0 && len(schemaDataTypeMapSlice) != 0:
		return loadDataTypeRuleUsingTableAndSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) != 0:
		return loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType, schemaDataTypeMapSlice)

	case len(tableDataTypeMapSlice) == 0 && len(schemaDataTypeMapSlice) == 0:
		return strings.ToUpper(buildInColumnType)
	default:
		panic(fmt.Errorf("oracle data type mapping failed, tableDataTypeMapSlice [%v],schemaDataTypeMapSlice [%v]", len(tableDataTypeMapSlice), len(schemaDataTypeMapSlice)))
	}
}

func loadDataTypeRuleUsingTableAndSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableRuleMap, schemaDataTypeMapSlice []service.SchemaRuleMap) string {
	// 规则判断
	customTableDataType := loadDataTypeRuleOnlyUsingTable(originColumnType, buildInColumnType, tableDataTypeMapSlice)

	customSchemaDataType := loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType, schemaDataTypeMapSlice)

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
func loadDataTypeRuleOnlyUsingTable(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []service.TableRuleMap) string {
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
		if strings.EqualFold(tbl.ReverseMode, utils.ReverseModeO2M) {
			if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "NUMBER") {
				switch {
				case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
					if strings.EqualFold(strings.Replace(tbl.SourceColumnType, "*", "38", -1), originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
					if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				case !strings.Contains(strings.ToUpper(tbl.SourceColumnType), "(") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ")"):
					if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				default:
					if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				}
			} else {
				if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			}
		}

		if strings.EqualFold(tbl.ReverseMode, utils.ReverseModeM2O) {
			if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "YEAR") && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
			if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "REAL") && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
			if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
		}

	}
	return strings.ToUpper(buildInColumnType)
}

// 库级别自定义映射规则
func loadDataTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType string, schemaDataTypeMapSlice []service.SchemaRuleMap) string {
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
		if strings.EqualFold(tbl.ReverseMode, utils.ReverseModeO2M) {
			if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "NUMBER") {
				switch {
				case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
					if strings.EqualFold(strings.Replace(tbl.SourceColumnType, "*", "38", -1), originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
					if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				case !strings.Contains(strings.ToUpper(tbl.SourceColumnType), "(") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ")"):
					if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
						tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				default:
					if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				}
			} else {
				if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			}
		}

		if strings.EqualFold(tbl.ReverseMode, utils.ReverseModeM2O) {
			if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "YEAR") && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
			if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "REAL") && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
			if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
				return strings.ToUpper(tbl.TargetColumnType)
			}
		}

	}
	return strings.ToUpper(buildInColumnType)
}

// 字段级别自定义映射规则
func loadDataTypeRuleOnlyUsingColumn(columnName string, originColumnType string, buildInColumnType string, columnDataTypeMapSlice []service.ColumnRuleMap) string {
	if len(columnDataTypeMapSlice) == 0 {
		return buildInColumnType
	}
	for _, tbl := range columnDataTypeMapSlice {
		if strings.EqualFold(tbl.SourceColumnName, columnName) {
			/*
				number 类型处理：函数匹配 ->  GetOracleTableColumn
				- number(*,10) -> number(38,10)
				- number(*,0) -> number(38,0)
				- number(*) -> number(38,127)
				- number -> number(38,127)
				- number(5) -> number(5)
				- number(8,9) -> number(8,9)
			*/
			if strings.EqualFold(tbl.ReverseMode, utils.ReverseModeO2M) {
				if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "NUMBER") {
					switch {
					case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
						if strings.EqualFold(strings.Replace(tbl.SourceColumnType, "*", "38", -1), originColumnType) &&
							tbl.TargetColumnType != "" {
							return strings.ToUpper(tbl.TargetColumnType)
						}
					case strings.Contains(strings.ToUpper(tbl.SourceColumnType), "*") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ","):
						if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
							tbl.TargetColumnType != "" {
							return strings.ToUpper(tbl.TargetColumnType)
						}
					case !strings.Contains(strings.ToUpper(tbl.SourceColumnType), "(") && !strings.Contains(strings.ToUpper(tbl.SourceColumnType), ")"):
						if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
							tbl.TargetColumnType != "" {
							return strings.ToUpper(tbl.TargetColumnType)
						}
					default:
						if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
							return strings.ToUpper(tbl.TargetColumnType)
						}
					}
				} else {
					if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
						return strings.ToUpper(tbl.TargetColumnType)
					}
				}
			}

			if strings.EqualFold(tbl.ReverseMode, utils.ReverseModeM2O) {
				if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "YEAR") && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
				if strings.Contains(strings.ToUpper(tbl.SourceColumnType), "REAL") && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
				if strings.EqualFold(tbl.SourceColumnType, originColumnType) && tbl.TargetColumnType != "" {
					return strings.ToUpper(tbl.TargetColumnType)
				}
			}
		}
	}
	return strings.ToUpper(buildInColumnType)
}
