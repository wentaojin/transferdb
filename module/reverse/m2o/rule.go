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
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"regexp"
	"strings"
)

type Rule struct {
	Ctx               context.Context     `json:"-"`
	SourceSchema      string              `json:"source_schema"`
	SourceTableName   string              `json:"source_table_name"`
	TargetSchema      string              `json:"target_schema"`
	TargetTableName   string              `json:"target_table_name"`
	PrimaryKeyINFO    []map[string]string `json:"primary_key_info"`
	UniqueKeyINFO     []map[string]string `json:"unique_key_info"`
	ForeignKeyINFO    []map[string]string `json:"foreign_key_info"`
	CheckKeyINFO      []map[string]string `json:"check_key_info"`
	UniqueIndexINFO   []map[string]string `json:"unique_index_info"`
	NormalIndexINFO   []map[string]string `json:"normal_index_info"`
	TableCommentINFO  []map[string]string `json:"table_comment_info"`
	TableColumnINFO   []map[string]string `json:"table_column_info"`
	ColumnCommentINFO []map[string]string `json:"column_comment_info"`
	IsPartition       bool                `json:"is_partition"`

	MySQL  *mysql.MySQL   `json:"-"`
	Oracle *oracle.Oracle `json:"-"`
	MetaDB *meta.Meta     `json:"-"`
}

func (r *Rule) GenCreateTableDDL() (reverseDDL string, checkKeyDDL []string, foreignKeyDDL []string, compatibleDDL []string, err error) {
	targetSchema, targetTable := r.GenTablePrefix()

	tableColumnMetas, err := r.GenTableColumn()
	if err != nil {
		return reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, err
	}

	tableKeyMetas, compDDL, err := r.GenTableKeyMeta()
	if err != nil {
		return reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, err
	}
	compatibleDDL = compDDL

	// table create sql -> target
	if !r.IsPartition {
		if len(tableKeyMetas) > 0 {
			reverseDDL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s,\n%s\n)",
				targetSchema, targetTable,
				strings.Join(tableColumnMetas, ",\n"),
				strings.Join(tableKeyMetas, ",\n"))
		} else {
			reverseDDL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s\n)",
				targetSchema, targetTable,
				strings.Join(tableColumnMetas, ",\n"))
		}
	} else {
		partitionINFO, err := r.MySQL.GetMySQLPartitionTableDetailINFO(r.SourceSchema, r.SourceTableName)
		if err != nil {
			return reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, err
		}
		if len(tableKeyMetas) > 0 {
			reverseDDL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s,\n%s\n) PARTITION BY %s;",
				targetSchema,
				targetTable,
				strings.Join(tableColumnMetas, ",\n"),
				strings.Join(tableKeyMetas, ",\n"),
				partitionINFO)
		} else {
			reverseDDL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s\n) PARTITION BY %s;",
				targetSchema,
				targetTable,
				strings.Join(tableColumnMetas, ",\n"),
				partitionINFO)
		}
	}

	// table check、foreign -> target
	checkKeyMetas, err := r.GenTableCheckKey()
	if err != nil {
		return reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, err
	}

	if len(checkKeyMetas) > 0 {
		for _, ck := range checkKeyMetas {
			ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", targetSchema, targetTable, ck)
			zap.L().Info("reverse",
				zap.String("schema", targetSchema),
				zap.String("table", targetTable),
				zap.String("ck sql", ckSQL))

			checkKeyDDL = append(checkKeyDDL, ckSQL)
		}
	}

	foreignKeyMetas, err := r.GenTableForeignKey()
	if err != nil {
		return reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, err
	}

	if len(foreignKeyDDL) > 0 {
		for _, fk := range foreignKeyMetas {
			addFkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", targetSchema, targetTable, fk)
			zap.L().Info("reverse",
				zap.String("schema", targetSchema),
				zap.String("table", targetTable),
				zap.String("fk sql", addFkSQL))
			foreignKeyDDL = append(foreignKeyDDL, addFkSQL)
		}
	}

	zap.L().Info("reverse oracle table struct", zap.String("table", r.String()))

	return reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, nil
}

func (r *Rule) GenTableKeyMeta() (tableKeyMetas []string, compatibilityIndexSQL []string, err error) {
	// 唯一约束/普通索引/唯一索引
	uniqueKeyMetas, err := r.GenTableUniqueKey()
	if err != nil {
		return tableKeyMetas, compatibilityIndexSQL, fmt.Errorf("table json [%v], mysql db reverse table unique constraint failed: %v", r.String(), err)
	}

	uniqueIndexMetas, uniqueIndexCompSQL, err := r.GenTableUniqueIndex()
	if err != nil {
		return tableKeyMetas, compatibilityIndexSQL, fmt.Errorf("table json [%v], mysql db reverse table key unique index failed: %v", r.String(), err)
	}

	if len(uniqueIndexCompSQL) > 0 {
		compatibilityIndexSQL = append(compatibilityIndexSQL, uniqueIndexCompSQL...)
	}

	// 主键
	primaryKeyMetas, err := r.GenTablePrimaryKey()
	if err != nil {
		return tableKeyMetas, compatibilityIndexSQL, err
	}

	if len(primaryKeyMetas) > 0 {
		tableKeyMetas = append(tableKeyMetas, primaryKeyMetas...)
	}

	if len(uniqueKeyMetas) > 0 {
		tableKeyMetas = append(tableKeyMetas, uniqueKeyMetas...)
	}

	if len(uniqueIndexMetas) > 0 {
		tableKeyMetas = append(tableKeyMetas, uniqueIndexMetas...)
	}

	return tableKeyMetas, compatibilityIndexSQL, nil
}

func (r *Rule) GenTablePrimaryKey() (primaryKeyMetas []string, err error) {
	if len(r.PrimaryKeyINFO) > 0 {
		for _, rowUKCol := range r.PrimaryKeyINFO {
			var uk string
			if rowUKCol["CONSTRAINT_TYPE"] == "PK" {
				uk = fmt.Sprintf("PRIMARY KEY (%s)", strings.ToUpper(rowUKCol["COLUMN_LIST"]))
			} else {
				return primaryKeyMetas, fmt.Errorf("table json [%v], error on get table primary key: %v", r.String(), err)
			}
			primaryKeyMetas = append(primaryKeyMetas, uk)
		}
	}
	return primaryKeyMetas, nil
}

func (r *Rule) GenTableUniqueKey() (uniqueKeyMetas []string, err error) {
	if len(r.UniqueKeyINFO) > 0 {
		for _, rowUKCol := range r.UniqueKeyINFO {
			var uk string
			if rowUKCol["CONSTRAINT_TYPE"] == "PK" {
				return uniqueKeyMetas, fmt.Errorf("table json [%v], error on get table primary key: %v", r.String(), err)
			} else {
				uk = fmt.Sprintf("CONSTRAINT %s UNIQUE (%s)",
					strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(rowUKCol["COLUMN_LIST"]))
			}
			uniqueKeyMetas = append(uniqueKeyMetas, uk)
		}
	}
	return uniqueKeyMetas, nil
}

func (r *Rule) GenTableForeignKey() (foreignKeyMetas []string, err error) {
	if len(r.ForeignKeyINFO) > 0 {
		for _, rowFKCol := range r.ForeignKeyINFO {
			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" || rowFKCol["DELETE_RULE"] == "RESTRICT" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON DELETE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON DELETE SET NULL",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
			if rowFKCol["UPDATE_RULE"] == "" || rowFKCol["UPDATE_RULE"] == "NO ACTION" || rowFKCol["UPDATE_RULE"] == "RESTRICT" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES `%s`.%s (%s) ON UPDATE %s",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]),
					strings.ToUpper(rowFKCol["UPDATE_RULE"]),
				)
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
			if rowFKCol["UPDATE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON UPDATE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
			if rowFKCol["UPDATE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON UPDATE SET NULL",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
		}
	}
	return foreignKeyMetas, nil
}

func (r *Rule) GenTableCheckKey() (checkKeyMetas []string, err error) {
	if len(r.CheckKeyINFO) > 0 {
		for _, rowFKCol := range r.CheckKeyINFO {
			ck := fmt.Sprintf("CONSTRAINT %s CHECK (%s)",
				strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
				strings.ToUpper(rowFKCol["SEARCH_CONDITION"]))
			checkKeyMetas = append(checkKeyMetas, ck)
		}
	}
	return checkKeyMetas, nil
}

func (r *Rule) GenTableUniqueIndex() (uniqueIndexMetas []string, compatibilityIndexSQL []string, err error) {
	// MySQL Unique Index = Unique Constraint
	return
}

func (r *Rule) GenTableNormalIndex() (normalIndexMetas []string, compatibilityIndexSQL []string, err error) {
	if len(r.NormalIndexINFO) > 0 {
		for _, kv := range r.NormalIndexINFO {
			var idx string
			if kv["UNIQUENESS"] == "UNIQUE" {
				idx = fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					r.TargetSchema,
					r.TargetTableName,
					strings.ToUpper(kv["COLUMN_LIST"]),
				)
			} else {
				idx = fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					r.TargetSchema,
					r.TargetTableName,
					strings.ToUpper(kv["COLUMN_LIST"]),
				)
			}
			normalIndexMetas = append(normalIndexMetas, idx)
		}
	}
	return normalIndexMetas, compatibilityIndexSQL, nil
}

func (r *Rule) GenTableComment() (tableComment string, err error) {
	if len(r.TableColumnINFO) > 0 && r.TableCommentINFO[0]["TABLE_COMMENT"] != "" {
		tableComment = fmt.Sprintf(`COMMENT ON TABLE %s.%s IS '%s';`, r.TargetSchema, r.TargetTableName, r.TableCommentINFO[0]["TABLE_COMMENT"])
	}
	return tableComment, nil
}

func (r *Rule) GenTableColumn() (columnMetas []string, err error) {
	reg, err := regexp.Compile(`^.+\(\)$`)
	if err != nil {
		return columnMetas, err
	}

	oracleExtendedMode, err := r.Oracle.GetOracleExtendedMode()
	if err != nil {
		return columnMetas, err
	}
	oracleDBVersion, err := r.Oracle.GetOracleDBVersion()
	if err != nil {
		return columnMetas, err
	}

	for _, rowCol := range r.TableColumnINFO {
		var (
			columnCollation string
			nullable        string
			dataDefault     string
			columnType      string
			columnName      string
		)
		// 字段排序规则检查
		if columnCollationMapVal, ok := common.MySQLDBCollationMap[strings.ToLower(rowCol["COLLATION_NAME"])]; ok {
			if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
				// oracle 12.2 版本及以上，字符集 columnCollation 开启需激活 extended 特性
				// columnCollation BINARY_CS : Both case and accent sensitive. This is default if no extension is used.
				switch {
				case strings.EqualFold(rowCol["COLLATION_NAME"], "utf8mb4_bin") || strings.EqualFold(rowCol["COLLATION_NAME"], "utf8_bin"):
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
			case !strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN"):
				return columnMetas, fmt.Errorf(`table json [%v], error on generate mysql table column column collation [%v]`, r.String(), rowCol["COLLATION_NAME"])
			case strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN"):
				// column columnCollation value UNKNOWN, 代表是非字符串数据类型
				// skip ignore
				columnCollation = ""
			default:
				return columnMetas, fmt.Errorf("mysql table column meta generate failed, column [%s] not support column collation [%s]", rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
			}
		}

		// Special M2O
		// MySQL/TiDB default value character insensitive
		var regDataDefault string
		if common.IsContainString(common.SpecialMySQLDataDefaultsWithDataTYPE, common.StringUPPER(rowCol["DATA_TYPE"])) {
			if reg.MatchString(rowCol["DATA_DEFAULT"]) || strings.EqualFold(rowCol["DATA_DEFAULT"], "CURRENT_TIMESTAMP") || strings.EqualFold(rowCol["DATA_DEFAULT"], "") {
				regDataDefault = rowCol["DATA_DEFAULT"]
			} else {
				regDataDefault = common.StringsBuilder(`'`, rowCol["DATA_DEFAULT"], `'`)
			}
		} else {
			regDataDefault = rowCol["DATA_DEFAULT"]
		}
		if common.IsContainString(common.SpecialMySQLColumnCollationWithDataTYPE, common.StringUPPER(rowCol["DATA_TYPE"])) {
			// M2O reverse mysql table to oracle table  special data type configure columnCollation error
			// ORA-43912: invalid columnCollation specified for a CLOB or NCLOB value
			// columncolumnCollation = ""
			columnCollation = ""
		}

		if strings.EqualFold(rowCol["NULLABLE"], "Y") {
			nullable = "NULL"
		} else {
			nullable = "NOT NULL"
		}

		if !strings.EqualFold(regDataDefault, "") {
			dataDefault, err = r.ChangeTableColumnDefaultValue(regDataDefault)
			if err != nil {
				return columnMetas, err
			}
		} else {
			dataDefault = regDataDefault
		}

		columnName = rowCol["COLUMN_NAME"]
		columnType, err = r.ChangeTableColumnType(r.SourceSchema, r.SourceTableName, columnName, Column{
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
		})
		if err != nil {
			return columnMetas, err
		}

		if strings.EqualFold(nullable, "NULL") {
			// M2O
			switch {
			case columnCollation != "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s COLLATE %s DEFAULT %s", columnName, columnType, columnCollation, dataDefault))
			case columnCollation != "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s COLLATE %s", columnName, columnType, columnCollation))
			case columnCollation == "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s DEFAULT %s", columnName, columnType, dataDefault))
			case columnCollation == "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s", columnName, columnType))
			default:
				return columnMetas, fmt.Errorf("error on gen mysql schema table column meta with nullable, rule: %v", r.String())
			}
		} else {
			// M2O
			switch {
			case columnCollation != "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s COLLATE %s DEFAULT %s %s", columnName, columnType, columnCollation, dataDefault, nullable))
			case columnCollation != "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s COLLATE %s %s", columnName, columnType, columnCollation, nullable))
			case columnCollation == "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s DEFAULT %s %s", columnName, columnType, dataDefault, nullable))
			case columnCollation == "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s %s", columnName, columnType, nullable))
			default:
				return columnMetas, fmt.Errorf("error on gen mysql schema table column meta without nullable, rule: %v", r.String())
			}
		}
	}

	return columnMetas, nil
}

func (r *Rule) GenTableColumnComment() (columnComments []string, err error) {
	if len(r.TableColumnINFO) > 0 {
		for _, rowCol := range r.TableColumnINFO {
			if rowCol["COMMENTS"] != "" {
				columnComments = append(columnComments, fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS '%s';`, r.TargetSchema, r.TargetTableName, rowCol["COLUMN_NAME"], common.SpecialLettersUsingOracle([]byte(rowCol["COMMENTS"]))))
			}
		}
	}
	return columnComments, nil
}

func (r *Rule) GenTablePrefix() (string, string) {
	targetSchema := r.ChangeSchemaName()
	targetTable := r.ChangeTableName()

	return targetSchema, targetTable
}

func (r *Rule) ChangeSchemaName() string {
	if r.TargetSchema == "" {
		return r.SourceSchema
	}
	if r.TargetSchema != "" {
		return r.TargetSchema
	}
	return r.SourceSchema
}

func (r *Rule) ChangeTableName() string {
	if r.TargetTableName == "" {
		return r.SourceTableName
	}
	if r.TargetTableName != "" {
		return r.TargetTableName
	}
	return r.SourceTableName
}

// 数据库查询获取自定义表结构转换规则
// 加载数据类型转换规则【处理字段级别、表级别、库级别数据类型映射规则】
// 数据类型转换规则判断，未设置自定义规则，默认采用内置默认字段类型转换
func (r *Rule) ChangeTableColumnType(sourceSchema, sourceTable, sourceColumn string, column interface{}) (string, error) {
	var columnType string
	// 获取内置映射规则
	buildinDatatypeNames, err := meta.NewBuildinDatatypeRuleModel(r.MetaDB).BatchQueryBuildinDatatype(r.Ctx, &meta.BuildinDatatypeRule{
		DBTypeS: common.TaskDBMySQL,
		DBTypeT: common.TaskDBOracle,
	})
	if err != nil {
		return columnType, err
	}
	originColumnType, buildInColumnType, err := MySQLTableColumnMapRule(sourceSchema, sourceTable, column.(Column), buildinDatatypeNames)
	if err != nil {
		return columnType, err
	}
	// 获取自定义映射规则
	columnDataTypeMapSlice, err := meta.NewColumnDatatypeRuleModel(r.MetaDB).DetailColumnRule(r.Ctx, &meta.ColumnDatatypeRule{
		DBTypeS:     common.TaskDBMySQL,
		DBTypeT:     common.TaskDBOracle,
		SchemaNameS: r.SourceSchema,
		TableNameS:  r.SourceTableName,
		ColumnNameS: sourceColumn,
	})
	if err != nil {
		return columnType, err
	}

	tableDataTypeMapSlice, err := meta.NewTableDatatypeRuleModel(r.MetaDB).DetailTableRule(r.Ctx, &meta.TableDatatypeRule{
		DBTypeS:     common.TaskDBMySQL,
		DBTypeT:     common.TaskDBOracle,
		SchemaNameS: r.SourceSchema,
		TableNameS:  r.SourceTableName,
	})
	if err != nil {
		return columnType, err
	}

	schemaDataTypeMapSlice, err := meta.NewSchemaDatatypeRuleModel(r.MetaDB).DetailSchemaRule(r.Ctx, &meta.SchemaDatatypeRule{
		DBTypeS:     common.TaskDBMySQL,
		DBTypeT:     common.TaskDBOracle,
		SchemaNameS: r.SourceSchema,
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
	columnTypeFromColumn := loadColumnTypeRuleOnlyUsingColumn(sourceColumn, originColumnType, buildInColumnType, columnDataTypeMapSlice)

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

func (r *Rule) ChangeTableColumnDefaultValue(dataDefault string) (string, error) {
	var defaultVal string
	defaultValueMapSlice, err := meta.NewBuildinColumnDefaultvalModel(r.MetaDB).DetailColumnDefaultVal(r.Ctx, &meta.BuildinColumnDefaultval{
		DBTypeS: common.TaskDBMySQL,
		DBTypeT: common.TaskDBOracle,
	})
	if err != nil {
		return defaultVal, err
	}
	return loadColumnDefaultValueRule(dataDefault, defaultValueMapSlice), nil
}

func (r *Rule) String() string {
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr)
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
