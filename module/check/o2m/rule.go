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
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/model"
	"github.com/wentaojin/transferdb/module/query/oracle"
	"strconv"
	"strings"
)

/*
Oracle 表字段映射转换 -> Check 阶段
*/
func GenOracleTableColumnMeta(ctx context.Context, oracle *oracle.Oracle, sourceSchema, sourceTableName, columnName string, columnINFO Column) (string, error) {
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

	if columnINFO.DataDefault != "" {
		dataDefault, err = ChangeTableColumnDefaultValue(ctx, oracle, columnINFO.DataDefault)
		if err != nil {
			return columnMeta, err
		}
	} else {
		dataDefault = columnINFO.DataDefault
	}

	columnType, err := ChangeTableColumnType(ctx, oracle, sourceSchema, sourceTableName, columnName, columnINFO)
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
func ChangeTableColumnType(ctx context.Context, oracle *oracle.Oracle, sourceSchema, sourceTableName, columnName string, columnINFO Column) (string, error) {
	var columnType string
	// 获取内置映射规则
	originColumnType, buildInColumnType, err := oracleTableColumnMapRuleReverse(columnINFO)
	if err != nil {
		return columnType, err
	}
	// 获取自定义映射规则
	columnDataTypeMapSlice, err := model.NewReverseModel(oracle.GormDB).ColumnRuleMap.Detail(ctx, &model.ColumnRuleMap{
		SourceSchemaName: sourceSchema,
		SourceTableName:  sourceTableName,
		SourceColumnName: columnName,
		ReverseMode:      common.ReverseO2MMode,
	})
	if err != nil {
		return columnType, err
	}

	tableDataTypeMapSlice, err := model.NewReverseModel(oracle.GormDB).TableRuleMap.Detail(ctx, &model.TableRuleMap{
		SourceSchemaName: sourceSchema,
		SourceTableName:  sourceTableName,
		ReverseMode:      common.ReverseO2MMode,
	})
	if err != nil {
		return columnType, err
	}

	schemaDataTypeMapSlice, err := model.NewReverseModel(oracle.GormDB).SchemaRuleMap.Detail(ctx, &model.SchemaRuleMap{
		SourceSchemaName: sourceSchema,
		ReverseMode:      common.ReverseO2MMode,
	})
	if err != nil {
		return columnType, err
	}

	// 优先级
	// column > table > schema > buildin
	if len(columnDataTypeMapSlice.([]model.ColumnRuleMap)) == 0 {
		return loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType,
			tableDataTypeMapSlice.([]model.TableRuleMap), schemaDataTypeMapSlice.([]model.SchemaRuleMap)), nil
	}

	// only column rule
	columnTypeFromColumn := loadColumnTypeRuleOnlyUsingColumn(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice.([]model.ColumnRuleMap))

	// table or schema rule check, return column type
	columnTypeFromOther := loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice.([]model.TableRuleMap), schemaDataTypeMapSlice.([]model.SchemaRuleMap))

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

func ChangeTableColumnDefaultValue(ctx context.Context, oracle *oracle.Oracle, dataDefault string) (string, error) {
	var defaultVal string
	defaultValueMapSlice, err := model.NewReverseModel(oracle.GormDB).DefaultValueMap.Detail(ctx, &model.DefaultValueMap{
		ReverseMode: common.ReverseO2MMode,
	})
	if err != nil {
		return defaultVal, err
	}
	return loadColumnDefaultValueRule(dataDefault, defaultValueMapSlice.([]model.DefaultValueMap)), nil
}

func oracleTableColumnMapRuleReverse(columnINFO Column) (string, string, error) {
	var (
		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
	)
	dataLength, err := strconv.Atoi(columnINFO.DataLength)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema table reverser column data_length string to int failed: %v", err)
	}
	dataPrecision, err := strconv.Atoi(columnINFO.DataPrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema table reverser column data_precision string to int failed: %v", err)
	}
	dataScale, err := strconv.Atoi(columnINFO.DataScale)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("oracle schema table reverser column data_scale string to int failed: %v", err)
	}

	switch strings.ToUpper(columnINFO.DataType) {
	case "NUMBER":
		switch {
		case dataScale > 0:
			switch {
			// oracle 真实数据类型 number(*) -> number(38,127)
			// number  -> number(38,127)
			// number(*,x) ->  number(38,x)
			// decimal(x,y) -> y max 30
			case dataPrecision == 38 && dataScale > 30:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, 30)
			case dataPrecision == 38 && dataScale <= 30:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, dataScale)
			default:
				if dataScale <= 30 {
					originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
					buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
				} else {
					originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
					buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, 30)
				}
			}
		case dataScale == 0:
			switch {
			case dataPrecision >= 1 && dataPrecision < 3:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "TINYINT"
			case dataPrecision >= 3 && dataPrecision < 5:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "SMALLINT"
			case dataPrecision >= 5 && dataPrecision < 9:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "INT"
			case dataPrecision >= 9 && dataPrecision < 19:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = "BIGINT"
			case dataPrecision >= 19 && dataPrecision <= 38:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d)", dataPrecision)
			default:
				originColumnType = fmt.Sprintf("NUMBER(%d)", dataPrecision)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d)", 65)
			}
		}

		return originColumnType, buildInColumnType, nil
	case "BFILE":
		originColumnType = "BFILE"
		buildInColumnType = "VARCHAR(255)"

		return originColumnType, buildInColumnType, nil
	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}

		return originColumnType, buildInColumnType, nil
	case "CHARACTER":
		originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}

		return originColumnType, buildInColumnType, nil
	case "CLOB":
		originColumnType = "CLOB"
		buildInColumnType = "LONGTEXT"

		return originColumnType, buildInColumnType, nil
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"

		return originColumnType, buildInColumnType, nil
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATETIME"

		return originColumnType, buildInColumnType, nil
	case "DECIMAL":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		return originColumnType, buildInColumnType, nil
	case "DEC":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}

		return originColumnType, buildInColumnType, nil

	case "DOUBLE PRECISION":
		originColumnType = "DOUBLE PRECISION"
		buildInColumnType = "DOUBLE PRECISION"

		return originColumnType, buildInColumnType, nil

	case "FLOAT":
		originColumnType = "FLOAT"
		if dataPrecision == 0 {
			buildInColumnType = "FLOAT"
		} else {
			buildInColumnType = "DOUBLE"
		}

		return originColumnType, buildInColumnType, nil
	case "INTEGER":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"

		return originColumnType, buildInColumnType, nil
	case "INT":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"

		return originColumnType, buildInColumnType, nil

	case "LONG":
		originColumnType = "LONG"
		buildInColumnType = "LONGTEXT"

		return originColumnType, buildInColumnType, nil
	case "LONG RAW":
		originColumnType = "LONG RAW"
		buildInColumnType = "LONGBLOB"

		return originColumnType, buildInColumnType, nil
	case "BINARY_FLOAT":
		originColumnType = "BINARY_FLOAT"
		buildInColumnType = "DOUBLE"

		return originColumnType, buildInColumnType, nil

	case "BINARY_DOUBLE":
		originColumnType = "BINARY_DOUBLE"
		buildInColumnType = "DOUBLE"

		return originColumnType, buildInColumnType, nil
	case "NCHAR":
		originColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		}

		return originColumnType, buildInColumnType, nil
	case "NCHAR VARYING":
		originColumnType = "NCHAR VARYING"
		buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "NCLOB":
		originColumnType = "NCLOB"
		buildInColumnType = "TEXT"

		return originColumnType, buildInColumnType, nil
	case "NUMERIC":
		originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)

		return originColumnType, buildInColumnType, nil

	case "NVARCHAR2":
		originColumnType = fmt.Sprintf("NVARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "RAW":
		originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		// https://ixyzero.com/blog/archives/2118.html
		//if dataLength < 256 {
		//	buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		//} else {
		//	buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		//}
		buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "REAL":
		originColumnType = "real"
		buildInColumnType = "DOUBLE"
		return originColumnType, buildInColumnType, nil

	case "ROWID":
		originColumnType = "ROWID"
		buildInColumnType = "CHAR(10)"

		return originColumnType, buildInColumnType, nil
	case "SMALLINT":
		originColumnType = "SMALLINT"
		buildInColumnType = "DECIMAL(38)"
		return originColumnType, buildInColumnType, nil

	case "UROWID":
		originColumnType = "UROWID"
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "VARCHAR2":
		originColumnType = fmt.Sprintf("VARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)

		return originColumnType, buildInColumnType, nil
	case "XMLTYPE":
		originColumnType = "XMLTYPE"
		buildInColumnType = "LONGTEXT"

		return originColumnType, buildInColumnType, nil
	default:
		if strings.Contains(columnINFO.DataType, "INTERVAL") {
			originColumnType = columnINFO.DataType
			buildInColumnType = "VARCHAR(30)"

			return originColumnType, buildInColumnType, nil
		} else if strings.Contains(columnINFO.DataType, "TIMESTAMP") {
			originColumnType = columnINFO.DataType
			if strings.Contains(columnINFO.DataType, "WITH TIME ZONE") || strings.Contains(columnINFO.DataType, "WITH LOCAL TIME ZONE") {
				if dataScale <= 6 {
					buildInColumnType = fmt.Sprintf("DATETIME(%d)", dataScale)
				} else {
					buildInColumnType = fmt.Sprintf("DATETIME(%d)", 6)
				}
			} else {
				if dataScale <= 6 {
					buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", dataScale)
				} else {
					buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", 6)
				}
			}
			return originColumnType, buildInColumnType, nil
		} else {
			originColumnType = columnINFO.DataType
			buildInColumnType = "TEXT"

			return originColumnType, buildInColumnType, nil
		}
	}
	return originColumnType, buildInColumnType, nil
}

func loadColumnDefaultValueRule(defaultValue string, defaultValueMapSlice []model.DefaultValueMap) string {
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

func loadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []model.TableRuleMap,
	schemaDataTypeMapSlice []model.SchemaRuleMap) string {
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

func loadDataTypeRuleUsingTableAndSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []model.TableRuleMap, schemaDataTypeMapSlice []model.SchemaRuleMap) string {
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
func loadColumnTypeRuleOnlyUsingTable(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []model.TableRuleMap) string {
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
		if strings.EqualFold(tbl.ReverseMode, common.ReverseO2MMode) {
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
	}
	return strings.ToUpper(buildInColumnType)
}

// 库级别自定义映射规则
func loadColumnTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType string, schemaDataTypeMapSlice []model.SchemaRuleMap) string {
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
		if strings.EqualFold(tbl.ReverseMode, common.ReverseO2MMode) {
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
	}
	return strings.ToUpper(buildInColumnType)
}

// 字段级别自定义映射规则
func loadColumnTypeRuleOnlyUsingColumn(columnName string, originColumnType string, buildInColumnType string, columnDataTypeMapSlice []model.ColumnRuleMap) string {
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
			if strings.EqualFold(tbl.ReverseMode, common.ReverseO2MMode) {
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
		}
	}
	return strings.ToUpper(buildInColumnType)
}

/*
	Oracle 表规则映射检查
*/

func OracleTableColumnMapRuleCheck(
	sourceSchema, targetSchema, tableName, columnName string,
	oracleColInfo, mysqlColInfo Column) (string, table.Row, error) {
	var tableRows table.Row

	// 字段精度类型转换
	oracleDataLength, err := strconv.Atoi(oracleColInfo.DataLength)
	if err != nil {
		return "", nil, fmt.Errorf("oracle schema [%s] table [%s] column data_length string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataPrecision, err := strconv.Atoi(oracleColInfo.DataPrecision)
	if err != nil {
		return "", nil, fmt.Errorf("oracle schema [%s] table [%s] column data_precision string to int failed: %v", sourceSchema, tableName, err)
	}
	oracleDataScale, err := strconv.Atoi(oracleColInfo.DataScale)
	if err != nil {
		return "", nil, fmt.Errorf("oracle schema [%s] table [%s] column data_scale string to int failed: %v", sourceSchema, tableName, err)
	}

	mysqlDataLength, err := strconv.Atoi(mysqlColInfo.DataLength)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] column data_length string to int failed: %v",
			targetSchema, tableName, err)
	}
	mysqlDataPrecision, err := strconv.Atoi(mysqlColInfo.DataPrecision)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] reverser column data_precision string to int failed: %v", targetSchema, tableName, err)
	}
	mysqlDataScale, err := strconv.Atoi(mysqlColInfo.DataScale)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] reverser column data_scale string to int failed: %v", targetSchema, tableName, err)
	}
	mysqlDatetimePrecision, err := strconv.Atoi(mysqlColInfo.DatetimePrecision)
	if err != nil {
		return "", nil, fmt.Errorf("mysql schema table [%s.%s] reverser column datetime_precision string to int failed: %v", targetSchema, tableName, err)
	}
	// 字段默认值、注释判断
	mysqlDataType := strings.ToUpper(mysqlColInfo.DataType)
	oracleDataType := strings.ToUpper(oracleColInfo.DataType)
	var (
		fixedMsg             string
		oracleColumnCharUsed string
	)

	if oracleColInfo.CharUsed == "C" {
		oracleColumnCharUsed = "char"
	} else if oracleColInfo.CharUsed == "B" {
		oracleColumnCharUsed = "bytes"
	} else {
		oracleColumnCharUsed = "unknown"
	}

	// GBK 处理，统一 UTF8MB4 处理
	var (
		mysqlCharacterSet string
		mysqlCollation    string
	)
	if strings.ToUpper(common.OracleDBCharacterSetMap[oracleColInfo.CharacterSet]) == "GBK" {
		mysqlCharacterSet = strings.ToLower("UTF8MB4")
	} else {
		mysqlCharacterSet = strings.ToLower(common.OracleDBCharacterSetMap[oracleColInfo.CharacterSet])
	}
	mysqlCollation = strings.ToLower(common.OracleCollationMap[oracleColInfo.Collation])

	oracleDiffColMeta := genColumnNullCommentDefaultMeta(oracleColInfo.NULLABLE, oracleColInfo.Comment, oracleColInfo.DataDefault)
	mysqlDiffColMeta := genColumnNullCommentDefaultMeta(mysqlColInfo.NULLABLE, mysqlColInfo.Comment, mysqlColInfo.DataDefault)

	oracleColMeta := genColumnNullCommentDefaultMeta(oracleColInfo.NULLABLE, oracleColInfo.Comment, oracleColInfo.OracleOriginDataDefault)
	mysqlColMeta := genColumnNullCommentDefaultMeta(mysqlColInfo.NULLABLE, mysqlColInfo.Comment, mysqlColInfo.MySQLOriginDataDefault)

	// 字段类型判断
	// CHARACTER SET %s COLLATE %s（Only 作用于字符类型）
	switch oracleDataType {
	// 数字
	case "NUMBER":
		switch {
		case oracleDataScale > 0:
		// oracle 真实数据类型 number(*) -> number(38,127)
		// number  -> number(38,127)
		// number(*,x) ->  number(38,x)
		// decimal(x,y) -> y max 30
		case oracleDataPrecision == 38 && oracleDataScale > 30:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 65 && mysqlDataScale == 30 && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}

			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL(65,30) %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"DECIMAL(65,30)",
				oracleColMeta,
			)

		case oracleDataPrecision == 38 && oracleDataScale <= 30:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 65 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}

			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL(65,%d) %s", oracleDataScale, oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(65,%d)", oracleDataScale),
				oracleColMeta,
			)
		default:
			if oracleDataScale <= 30 {
				if mysqlDataType == "DECIMAL" && oracleDataPrecision == mysqlDataPrecision && oracleDataScale == mysqlDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}

				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
					fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
					fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
					oracleColMeta,
				)
			} else {
				if mysqlDataType == "DECIMAL" && oracleDataPrecision == mysqlDataPrecision && mysqlDataScale == 30 && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}

				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
					fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
					fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, 30, oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, 30),
					oracleColMeta,
				)
			}
		case oracleDataScale == 0:
			switch {
			case oracleDataPrecision >= 1 && oracleDataPrecision < 3:
				if mysqlDataType == "TINYINT" && mysqlDataPrecision >= 3 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}

				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("TINYINT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"TINYINT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 3 && oracleDataPrecision < 5:
				if mysqlDataType == "SMALLINT" && mysqlDataPrecision >= 5 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("SMALLINT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"SMALLINT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 5 && oracleDataPrecision < 9:
				if mysqlDataType == "INT" && mysqlDataPrecision >= 9 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("INT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"INT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 9 && oracleDataPrecision < 19:
				if mysqlDataType == "BIGINT" && mysqlDataPrecision >= 19 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("BIGINT %s", oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"BIGINT",
					oracleColMeta,
				)
			case oracleDataPrecision >= 19 && oracleDataPrecision <= 38:
				if mysqlDataType == "DECIMAL" && mysqlDataPrecision >= 19 && mysqlDataPrecision <= 38 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataPrecision, mysqlColMeta),
					fmt.Sprintf("DECIMAL(%d) %s", oracleDataPrecision, oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					fmt.Sprintf("DECIMAL(%d)", oracleDataPrecision),
					oracleColMeta,
				)
			default:
				if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 65 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
					return "", nil, nil
				}
				tableRows = table.Row{tableName, columnName,
					fmt.Sprintf("NUMBER(%d) %s", oracleDataPrecision, oracleColMeta),
					fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
					fmt.Sprintf("DECIMAL(%d) %s", 65, oracleColMeta)}

				fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
					targetSchema,
					tableName,
					columnName,
					"DECIMAL(65)",
					oracleColMeta,
				)
			}
		}
		return fixedMsg, tableRows, nil
	case "DECIMAL":
		switch {
		case oracleDataScale == 0 && oracleDataPrecision == 0:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 10 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL %s", oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"DECIMAL",
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		default:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == oracleDataPrecision && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
	case "DEC":
		switch {
		case oracleDataScale == 0 && oracleDataPrecision == 0:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 10 && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL %s", oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"DECIMAL",
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		default:
			if mysqlDataType == "DECIMAL" && mysqlDataPrecision == oracleDataPrecision && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
	case "DOUBLE PRECISION":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("DOUBLE PRECISION %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE PRECISION %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE PRECISION",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "FLOAT":
		if oracleDataPrecision == 0 {
			if mysqlDataType == "FLOAT" && mysqlDataPrecision == 12 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("FLOAT %s", oracleColMeta),
				fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
				fmt.Sprintf("FLOAT %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"FLOAT",
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("FLOAT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "INTEGER":
		if mysqlDataType == "INT" && mysqlDataPrecision >= 10 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("INTEGER %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("INT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"INT",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "INT":
		if mysqlDataType == "INT" && mysqlDataPrecision >= 10 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("INT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("INT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"INT",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "REAL":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("REAL %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NUMERIC":
		if mysqlDataType == "DECIMAL" && mysqlDataPrecision == oracleDataPrecision && mysqlDataScale == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NUMERIC(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DECIMAL(%d,%d) %s", oracleDataPrecision, oracleDataScale, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("DECIMAL(%d,%d)", oracleDataPrecision, oracleDataScale),
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "BINARY_FLOAT":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BINARY_FLOAT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "BINARY_DOUBLE":
		if mysqlDataType == "DOUBLE" && mysqlDataPrecision == 22 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BINARY_DOUBLE %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DOUBLE %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DOUBLE",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "SMALLINT":
		if mysqlDataType == "DECIMAL" && mysqlDataPrecision == 38 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("SMALLINT %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DECIMAL(38) %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DECIMAL(38)",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

	// 字符
	case "BFILE":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == 255 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BFILE %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(255) %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"VARCHAR(255)",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "CHARACTER":
		if oracleDataLength < 256 {
			if mysqlDataType == "CHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("CHARACTER(%d) %s", oracleDataLength, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("CHAR(%d) %s", oracleDataLength, oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("CHAR(%d)", oracleDataLength),
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("CHARACTER(%d) %s", oracleDataLength, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "LONG":
		if mysqlDataType == "LONGTEXT" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("LONG %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGTEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "LONG RAW":
		if mysqlDataType == "LONGBLOB" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("LONG RAW %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGBLOB %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGBLOB",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NCHAR VARYING":
		if mysqlDataType == "NCHAR VARYING" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NCHAR VARYING %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("NCHAR VARYING(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NCHAR VARYING(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NCLOB":
		if mysqlDataType == "TEXT" && mysqlDataLength == 65535 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NCLOB %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("TEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"TEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "RAW":
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		//if oracleDataLength < 256 {
		//	if mysqlDataType == "BINARY" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
		//		return "", nil, nil
		//	}
		//	tableRows = table.Row{tableName, columnName,
		//		fmt.Sprintf("RAW(%d) %s", oracleDataLength, oracleColMeta),
		//		fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
		//		fmt.Sprintf("BINARY(%d) %s", oracleDataLength, oracleColMeta)}
		//
		//	fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
		//		targetSchema,
		//		tableName,
		//		columnName,
		//		fmt.Sprintf("BINARY(%d)", oracleDataLength),
		//		mysqlCharacterSet,
		//		mysqlCollation,
		//		oracleColMeta,
		//	)
		//	return fixedMsg, tableRows, nil
		//}

		if mysqlDataType == "VARBINARY" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("RAW(%d) %s", oracleDataLength, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARBINARY(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARBINARY(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "ROWID":
		if mysqlDataType == "CHAR" && mysqlDataLength == 10 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("ROWID %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("CHAR(10) %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"CHAR(10)",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "UROWID":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("UROWID %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "VARCHAR":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "XMLTYPE":
		if mysqlDataType == "LONGTEXT" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("XMLTYPE %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGTEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

	// 二进制
	case "CLOB":
		if mysqlDataType == "LONGTEXT" && mysqlDataLength == 4294967295 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("CLOB %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("LONGTEXT %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"LONGTEXT",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "BLOB":
		if mysqlDataType == "BLOB" && mysqlDataLength == 65535 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("BLOB %s", oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("BLOB %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"BLOB",
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

	// 时间
	case "DATE":
		if mysqlDataType == "DATETIME" && mysqlDataLength == 0 && mysqlDataPrecision == 0 && mysqlDataScale == 0 && mysqlDatetimePrecision == 0 && oracleDiffColMeta == mysqlDiffColMeta {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("DATE %s", oracleColMeta),
			fmt.Sprintf("%s(%d,%d) %s", mysqlDataType, mysqlDataPrecision, mysqlDataScale, mysqlColMeta),
			fmt.Sprintf("DATETIME %s", oracleColMeta)}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			"DATETIME",
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

	// oracle 字符类型 bytes/char 判断 B/C
	// CHAR、NCHAR、VARCHAR2、NVARCHAR2( oracle 字符类型 B/C)
	// mysql 同等长度（data_length） char 字符类型 > oracle bytes 字节类型
	case "CHAR":
		if oracleDataLength < 256 {
			if mysqlDataType == "CHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("CHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("CHAR(%d) %s", oracleDataLength, oracleColMeta)}

			// 忽略 bytes -> char 语句修复输出
			if mysqlDataType == "CHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
				return fixedMsg, tableRows, nil
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("CHAR(%d)", oracleDataLength),
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("CHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}
		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NCHAR":
		if oracleDataLength < 256 {
			if mysqlDataType == "NCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("NCHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("NCHAR(%d) %s", oracleDataLength, oracleColMeta)}

			// 忽略 bytes -> char 语句修复输出
			if mysqlDataType == "NCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
				return fixedMsg, tableRows, nil
			}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				fmt.Sprintf("NCHAR(%d)", oracleDataLength),
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NVARCHAR(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("NVARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NVARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "VARCHAR2":
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("VARCHAR2(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("VARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "VARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("VARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil
	case "NVARCHAR2":
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta && oracleColumnCharUsed == "char" {
			return "", nil, nil
		}
		tableRows = table.Row{tableName, columnName,
			fmt.Sprintf("NVARCHAR2(%d %s) %s", oracleDataLength, oracleColumnCharUsed, oracleColMeta),
			fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
			fmt.Sprintf("NVARCHAR(%d) %s", oracleDataLength, oracleColMeta)}

		// 忽略 bytes -> char 语句修复输出
		if mysqlDataType == "NVARCHAR" && mysqlDataLength == oracleDataLength && oracleDiffColMeta == mysqlDiffColMeta {
			return fixedMsg, tableRows, nil
		}

		fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
			targetSchema,
			tableName,
			columnName,
			fmt.Sprintf("NVARCHAR(%d)", oracleDataLength),
			mysqlCharacterSet,
			mysqlCollation,
			oracleColMeta,
		)
		return fixedMsg, tableRows, nil

	// 默认其他类型
	default:
		if strings.Contains(oracleDataType, "INTERVAL") {
			if mysqlDataType == "VARCHAR" && mysqlDataLength == 30 && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("VARCHAR(30) %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"VARCHAR(30)",
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		} else if strings.Contains(oracleDataType, "TIMESTAMP") {
			if strings.Contains(oracleDataType, "WITH TIME ZONE") || strings.Contains(oracleDataType, "WITH LOCAL TIME ZONE") {
				if oracleDataScale <= 6 {
					if mysqlDataType == "DATETIME" && mysqlDatetimePrecision == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
						return "", nil, nil
					}
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("DATETIME(%d) %s", oracleDataScale, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("DATETIME(%d)", oracleDataScale),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				} else {
					// mysql/tidb 只支持精度 6，oracle 精度最大是 9，会检查出来但是保持原样
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("DATETIME(%d) %s", 6, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("DATETIME(%d)", 6),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				}
			} else {
				if oracleDataScale <= 6 {
					if mysqlDataType == "TIMESTAMP" && mysqlDatetimePrecision == oracleDataScale && oracleDiffColMeta == mysqlDiffColMeta {
						return "", nil, nil
					}
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("TIMESTAMP(%d) %s", oracleDataScale, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("TIMESTAMP(%d)", oracleDataScale),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				} else {
					// mysql/tidb 只支持精度 6，oracle 精度最大是 9，会检查出来但是保持原样
					tableRows = table.Row{tableName, columnName,
						fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
						fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDatetimePrecision, mysqlColMeta),
						fmt.Sprintf("TIMESTAMP(%d) %s", 6, oracleColMeta)}

					fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s %s;\n",
						targetSchema,
						tableName,
						columnName,
						fmt.Sprintf("TIMESTAMP(%d)", 6),
						oracleColMeta,
					)
					return fixedMsg, tableRows, nil
				}
			}
		} else {
			if mysqlDataType == "TEXT" && mysqlDataLength == 65535 && mysqlDataScale == 0 && oracleDiffColMeta == mysqlDiffColMeta {
				return "", nil, nil
			}
			tableRows = table.Row{tableName, columnName,
				fmt.Sprintf("%s %s", oracleDataType, oracleColMeta),
				fmt.Sprintf("%s(%d) %s", mysqlDataType, mysqlDataLength, mysqlColMeta),
				fmt.Sprintf("TEXT %s", oracleColMeta)}

			fixedMsg = fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s CHARACTER SET %s COLLATE %s %s;\n",
				targetSchema,
				tableName,
				columnName,
				"TEXT",
				mysqlCharacterSet,
				mysqlCollation,
				oracleColMeta,
			)
			return fixedMsg, tableRows, nil
		}
	}
}

func genColumnNullCommentDefaultMeta(dataNullable, comments, dataDefault string) string {
	var (
		colMeta string
	)

	if dataNullable == "NULL" {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("DEFAULT %s COMMENT '%s'", dataDefault, comments)
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("DEFAULT NULL COMMENT '%s'", comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("DEFAULT %s", dataDefault)
		case comments == "" && dataDefault == "":
			colMeta = "DEFAULT NULL"
		}
	} else {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s COMMENT '%s'", dataNullable, dataDefault, comments)
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("%s COMMENT '%s'", dataNullable, comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s", dataNullable, dataDefault)
		case comments == "" && dataDefault == "":
			colMeta = fmt.Sprintf("%s", dataNullable)
		}
	}
	return colMeta
}

func genTableColumnCollation(nlsComp string, oraCollation bool, schemaCollation, tableCollation, columnCollation string) (string, error) {
	var collation string
	if oraCollation {
		if columnCollation != "" {
			collation = strings.ToUpper(columnCollation)
			return collation, nil
		}
		if columnCollation == "" && tableCollation != "" {
			collation = strings.ToUpper(tableCollation)
			return collation, nil
		}
		if columnCollation == "" && tableCollation == "" && schemaCollation != "" {
			collation = strings.ToUpper(schemaCollation)
			return collation, nil
		}
		return collation,
			fmt.Errorf("oracle schema collation [%v] table collation [%v] column collation [%v] isn't support by getColumnCollation", schemaCollation, tableCollation, columnCollation)
	} else {
		collation = strings.ToUpper(nlsComp)
		return collation, nil
	}
}

func genTableCollation(nlsComp string, oracleCollation bool, schemaCollation, tableCollation string) (string, error) {
	var collation string
	if oracleCollation {
		if tableCollation != "" {
			collation = strings.ToUpper(tableCollation)
			return collation, nil
		}
		if tableCollation == "" && schemaCollation != "" {
			collation = strings.ToUpper(schemaCollation)
			return collation, nil
		}
		return collation,
			fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support by getColumnCollation", schemaCollation, tableCollation)
	} else {
		collation = strings.ToUpper(nlsComp)
		return collation, nil
	}
}
