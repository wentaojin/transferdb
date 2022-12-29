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
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"go.uber.org/zap"
	"strings"
)

type Rule struct {
	OracleDBVersion          string              `json:"oracle_db_version"`
	OracleExtendedMode       bool                `json:"oracle_extended_mode"`
	SourceSchema             string              `json:"source_schema"`
	SourceTableName          string              `json:"source_table_name"`
	TargetSchema             string              `json:"target_schema"`
	TargetTableName          string              `json:"target_table_name"`
	PrimaryKeyINFO           []map[string]string `json:"primary_key_info"`
	UniqueKeyINFO            []map[string]string `json:"unique_key_info"`
	ForeignKeyINFO           []map[string]string `json:"foreign_key_info"`
	CheckKeyINFO             []map[string]string `json:"check_key_info"`
	UniqueIndexINFO          []map[string]string `json:"unique_index_info"`
	NormalIndexINFO          []map[string]string `json:"normal_index_info"`
	TableCommentINFO         []map[string]string `json:"table_comment_info"`
	TableColumnINFO          []map[string]string `json:"table_column_info"`
	ColumnCommentINFO        []map[string]string `json:"column_comment_info"`
	ColumnDatatypeRule       map[string]string   `json:"column_datatype_rule"`
	ColumnDataDefaultvalRule map[string]string   `json:"column_data_defaultval_rule"`
	TablePartitionDetail     string              `json:"table_partition_detail"`
}

func (r *Rule) GenCreateTableDDL() (reverseDDL string, checkKeyDDL []string, foreignKeyDDL []string, compatibleDDL []string, err error) {
	targetSchema, targetTable := r.GenTableNamePrefix()

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
	if strings.EqualFold(r.TablePartitionDetail, "") {
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
		if len(tableKeyMetas) > 0 {
			reverseDDL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s,\n%s\n) PARTITION BY %s;",
				targetSchema,
				targetTable,
				strings.Join(tableColumnMetas, ",\n"),
				strings.Join(tableKeyMetas, ",\n"),
				r.TablePartitionDetail)
		} else {
			reverseDDL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s\n) PARTITION BY %s;",
				targetSchema,
				targetTable,
				strings.Join(tableColumnMetas, ",\n"),
				r.TablePartitionDetail)
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
			if common.VersionOrdinal(r.OracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
				// oracle 12.2 版本及以上，字符集 columnCollation 开启需激活 extended 特性
				// columnCollation BINARY_CS : Both case and accent sensitive. This is default if no extension is used.
				switch {
				case strings.EqualFold(rowCol["COLLATION_NAME"], "utf8mb4_bin") || strings.EqualFold(rowCol["COLLATION_NAME"], "utf8_bin"):
					columnCollationArr := strings.Split(columnCollationMapVal, "/")
					if r.OracleExtendedMode {
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

		columnName = rowCol["COLUMN_NAME"]

		if !strings.EqualFold(rowCol["DATA_DEFAULT"], "") {
			if val, ok := r.ColumnDataDefaultvalRule[columnName]; ok {
				dataDefault = val
			} else {
				return columnMetas, fmt.Errorf("mysql table [%s.%s] column [%s] default value isn't exist", r.SourceSchema, r.SourceTableName, columnName)
			}
		} else {
			dataDefault = rowCol["DATA_DEFAULT"]
		}

		if val, ok := r.ColumnDatatypeRule[columnName]; ok {
			columnType = val
		} else {
			return columnMetas, fmt.Errorf("mysql table [%s.%s] column [%s] data type isn't exist", r.SourceSchema, r.SourceTableName, columnName)
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

func (r *Rule) GenTableNamePrefix() (string, string) {
	targetSchema := r.GenSchemaName()
	targetTable := r.GenTableName()

	return targetSchema, targetTable
}

func (r *Rule) GenSchemaName() string {
	if r.TargetSchema == "" {
		return r.SourceSchema
	}
	if r.TargetSchema != "" {
		return r.TargetSchema
	}
	return r.SourceSchema
}

func (r *Rule) GenTableName() string {
	if r.TargetTableName == "" {
		return r.SourceTableName
	}
	if r.TargetTableName != "" {
		return r.TargetTableName
	}
	return r.SourceTableName
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
