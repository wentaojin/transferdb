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
	"strconv"
	"strings"
)

/*
Oracle 表字段映射转换 -> Check 阶段
*/
func GenOracleTableColumnMeta(ctx context.Context, metaDB *meta.Meta, sourceSchema, sourceTableName, columnName string, columnINFO Column) (string, error) {
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
		dataDefault, err = ChangeTableColumnDefaultValue(ctx, metaDB, columnINFO.DataDefault)
		if err != nil {
			return columnMeta, err
		}
	} else {
		dataDefault = columnINFO.DataDefault
	}

	columnType, err := ChangeTableColumnType(ctx, metaDB, sourceSchema, sourceTableName, columnName, columnINFO)
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
func ChangeTableColumnType(ctx context.Context, metaDB *meta.Meta, sourceSchema, sourceTableName, columnName string, columnINFO Column) (string, error) {
	var columnType string
	// 获取内置映射规则
	originColumnType, buildInColumnType, err := oracleTableColumnMapRuleReverse(columnINFO)
	if err != nil {
		return columnType, err
	}
	// 获取自定义映射规则
	columnDataTypeMapSlice, err := meta.NewColumnRuleMapModel(metaDB).DetailColumnRule(ctx, &meta.ColumnRuleMap{
		DBTypeS:     common.TaskDBOracle,
		DBTypeT:     common.TaskDBMySQL,
		SchemaNameS: sourceSchema,
		TableNameS:  sourceTableName,
		ColumnNameS: columnName,
	})
	if err != nil {
		return columnType, err
	}

	tableDataTypeMapSlice, err := meta.NewTableRuleMapModel(metaDB).DetailTableRule(ctx, &meta.TableRuleMap{
		DBTypeS:     common.TaskDBOracle,
		DBTypeT:     common.TaskDBMySQL,
		SchemaNameS: sourceSchema,
		TableNameS:  sourceTableName,
	})
	if err != nil {
		return columnType, err
	}

	schemaDataTypeMapSlice, err := meta.NewSchemaRuleMapModel(metaDB).DetailSchemaRule(ctx, &meta.SchemaRuleMap{
		DBTypeS:     common.TaskDBOracle,
		DBTypeT:     common.TaskDBMySQL,
		SchemaNameS: sourceSchema,
	})
	if err != nil {
		return columnType, err
	}

	// 优先级
	// column > table > schema > buildin
	if len(columnDataTypeMapSlice.([]meta.ColumnRuleMap)) == 0 {
		return loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType,
			tableDataTypeMapSlice.([]meta.TableRuleMap), schemaDataTypeMapSlice.([]meta.SchemaRuleMap)), nil
	}

	// only column rule
	columnTypeFromColumn := loadColumnTypeRuleOnlyUsingColumn(columnName, originColumnType, buildInColumnType, columnDataTypeMapSlice.([]meta.ColumnRuleMap))

	// table or schema rule check, return column type
	columnTypeFromOther := loadDataTypeRuleUsingTableOrSchema(originColumnType, buildInColumnType, tableDataTypeMapSlice.([]meta.TableRuleMap), schemaDataTypeMapSlice.([]meta.SchemaRuleMap))

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

func ChangeTableColumnDefaultValue(ctx context.Context, metaDB *meta.Meta, dataDefault string) (string, error) {
	// 处理 oracle 默认值 ('xxx') 或者 (xxx)
	if strings.HasPrefix(dataDefault, "(") && strings.HasSuffix(dataDefault, ")") {
		dataDefault = strings.TrimLeft(dataDefault, "(")
		dataDefault = strings.TrimRight(dataDefault, ")")
	}

	defaultValueMapSlice, err := meta.NewBuildinColumnDefaultvalModel(metaDB).DetailColumnDefaultVal(ctx, &meta.BuildinColumnDefaultval{
		DBTypeS: common.TaskDBOracle,
		DBTypeT: common.TaskDBMySQL,
	})
	if err != nil {
		return dataDefault, err
	}
	return loadColumnDefaultValueRule(dataDefault, defaultValueMapSlice.([]meta.BuildinColumnDefaultval)), nil
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
}

func loadColumnDefaultValueRule(defaultValue string, defaultValueMapSlice []meta.BuildinColumnDefaultval) string {
	if len(defaultValueMapSlice) == 0 {
		return defaultValue
	}

	for _, dv := range defaultValueMapSlice {
		if strings.EqualFold(strings.TrimSpace(dv.DefaultValueS), strings.TrimSpace(defaultValue)) && dv.DefaultValueT != "" {
			return dv.DefaultValueT
		}
	}
	return defaultValue
}

func loadDataTypeRuleUsingTableOrSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableRuleMap,
	schemaDataTypeMapSlice []meta.SchemaRuleMap) string {
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

func loadDataTypeRuleUsingTableAndSchema(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableRuleMap, schemaDataTypeMapSlice []meta.SchemaRuleMap) string {
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
func loadColumnTypeRuleOnlyUsingTable(originColumnType string, buildInColumnType string, tableDataTypeMapSlice []meta.TableRuleMap) string {
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
func loadColumnTypeRuleOnlyUsingSchema(originColumnType, buildInColumnType string, schemaDataTypeMapSlice []meta.SchemaRuleMap) string {
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
func loadColumnTypeRuleOnlyUsingColumn(columnName string, originColumnType string, buildInColumnType string, columnDataTypeMapSlice []meta.ColumnRuleMap) string {
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
