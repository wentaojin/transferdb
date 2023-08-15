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
	"regexp"
	"strings"
)

type Rule struct {
	*Table
	*Info
}

type Info struct {
	SourceTableDDL       string              `json:"-"` // 忽略
	PrimaryKeyINFO       []map[string]string `json:"primary_key_info"`
	UniqueKeyINFO        []map[string]string `json:"unique_key_info"`
	ForeignKeyINFO       []map[string]string `json:"foreign_key_info"`
	CheckKeyINFO         []map[string]string `json:"check_key_info"`
	UniqueIndexINFO      []map[string]string `json:"unique_index_info"`
	NormalIndexINFO      []map[string]string `json:"normal_index_info"`
	TableCommentINFO     []map[string]string `json:"table_comment_info"`
	TableColumnINFO      []map[string]string `json:"table_column_info"`
	ColumnCommentINFO    []map[string]string `json:"column_comment_info"`
	TablePartitionDetail string              `json:"table_partition_detail"`
}

func (r *Rule) GenCreateTableDDL() (interface{}, error) {
	targetSchema, targetTable := r.GenTablePrefix()

	tableColumnMetas, err := r.GenTableColumn()
	if err != nil {
		return nil, err
	}

	tableKeyMetas, compatibleDDL, err := r.GenTableKeys()
	if err != nil {
		return nil, err
	}

	tablePrefix := fmt.Sprintf("CREATE TABLE %s.%s", r.GenSchemaName(), r.GenTableName())

	tableSuffix, err := r.GenTableSuffix()
	if err != nil {
		return nil, err
	}

	checkKeyMetas, err := r.GenTableCheckKey()
	if err != nil {
		return nil, err
	}

	foreignKeys, err := r.GenTableForeignKey()
	if err != nil {
		return nil, err
	}

	tableComment, err := r.GenTableComment()
	if err != nil {
		return nil, err
	}

	columnComment, err := r.GenTableColumnComment()
	if err != nil {
		return nil, err
	}
	tableNormalIndex, compNormalIndex, err := r.GenTableNormalIndex()
	if err != nil {
		return nil, err
	}
	compatibleDDL = append(compatibleDDL, compNormalIndex...)

	return &DDL{
		SourceSchemaName:     r.SourceSchemaName,
		SourceTableName:      r.SourceTableName,
		SourceTableType:      "NORMAL", // MySQL/TiDB table type
		SourceTableDDL:       r.SourceTableDDL,
		TargetSchemaName:     targetSchema, // change schema name
		TargetTableName:      targetTable,  // change table name
		TablePrefix:          tablePrefix,
		TableColumns:         tableColumnMetas,
		TableKeys:            tableKeyMetas,
		TableIndexes:         tableNormalIndex,
		TableSuffix:          tableSuffix,
		TableComment:         tableComment,
		ColumnCommentDDL:     columnComment,
		TableCheckKeys:       checkKeyMetas,
		TableForeignKeys:     foreignKeys,
		TableCompatibleDDL:   compatibleDDL,
		TablePartitionDetail: r.TablePartitionDetail,
	}, nil
}

func (r *Rule) GenTableKeys() (tableKeyMetas []string, compatibilityIndexSQL []string, err error) {
	// 唯一约束/普通索引/唯一索引
	uniqueKeys, err := r.GenTableUniqueKey()
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
	primaryKeys, err := r.GenTablePrimaryKey()
	if err != nil {
		return tableKeyMetas, compatibilityIndexSQL, err
	}

	if len(primaryKeys) > 0 {
		tableKeyMetas = append(tableKeyMetas, primaryKeys...)
	}

	if len(uniqueKeys) > 0 {
		tableKeyMetas = append(tableKeyMetas, uniqueKeys...)
	}

	if len(uniqueIndexMetas) > 0 {
		tableKeyMetas = append(tableKeyMetas, uniqueIndexMetas...)
	}

	return tableKeyMetas, compatibilityIndexSQL, nil
}

func (r *Rule) GenTablePrimaryKey() (primaryKeys []string, err error) {
	if len(r.PrimaryKeyINFO) > 0 {
		for _, rowUKCol := range r.PrimaryKeyINFO {
			var uk, columnList string
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
				columnList = strings.ToLower(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
				columnList = strings.ToUpper(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
				columnList = rowUKCol["COLUMN_LIST"]
			}
			if strings.EqualFold(rowUKCol["CONSTRAINT_TYPE"], "PK") {
				uk = fmt.Sprintf("PRIMARY KEY (%s)", columnList)
			} else {
				return primaryKeys, fmt.Errorf("table json [%v], error on get table primary key: %v", r.String(), err)
			}
			primaryKeys = append(primaryKeys, uk)
		}
	}
	return primaryKeys, nil
}

func (r *Rule) GenTableUniqueKey() (uniqueKeys []string, err error) {
	if len(r.UniqueKeyINFO) > 0 {
		for _, rowUKCol := range r.UniqueKeyINFO {
			var uk, columnList string
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
				columnList = strings.ToLower(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
				columnList = strings.ToUpper(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
				columnList = rowUKCol["COLUMN_LIST"]
			}
			if strings.EqualFold(rowUKCol["CONSTRAINT_TYPE"], "PK") {
				return uniqueKeys, fmt.Errorf("table json [%v], error on get table primary key: %v", r.String(), err)
			} else {
				uk = fmt.Sprintf("CONSTRAINT %s UNIQUE (%s)",
					rowUKCol["CONSTRAINT_NAME"], columnList)
			}
			uniqueKeys = append(uniqueKeys, uk)
		}
	}
	return uniqueKeys, nil
}

func (r *Rule) GenTableForeignKey() (foreignKeys []string, err error) {
	if len(r.ForeignKeyINFO) > 0 {
		var (
			columnList  string
			rOwner      string
			rTable      string
			rColumnList string
		)
		for _, rowFKCol := range r.ForeignKeyINFO {
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
				columnList = strings.ToLower(rowFKCol["COLUMN_LIST"])
				rOwner = strings.ToLower(rowFKCol["R_OWNER"])
				rTable = strings.ToLower(rowFKCol["RTABLE_NAME"])
				rColumnList = strings.ToLower(rowFKCol["RCOLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
				columnList = strings.ToUpper(rowFKCol["COLUMN_LIST"])
				rOwner = strings.ToUpper(rowFKCol["R_OWNER"])
				rTable = strings.ToUpper(rowFKCol["RTABLE_NAME"])
				rColumnList = strings.ToUpper(rowFKCol["RCOLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
				columnList = rowFKCol["COLUMN_LIST"]
				rOwner = rowFKCol["R_OWNER"]
				rTable = rowFKCol["RTABLE_NAME"]
				rColumnList = rowFKCol["RCOLUMN_LIST"]
			}
			if strings.EqualFold(rowFKCol["DELETE_RULE"], "") || strings.EqualFold(rowFKCol["DELETE_RULE"], "NO ACTION") || strings.EqualFold(rowFKCol["DELETE_RULE"], "RESTRICT") {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON DELETE CASCADE",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON DELETE SET NULL",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
				foreignKeys = append(foreignKeys, fk)
			}
			if strings.EqualFold(rowFKCol["UPDATE_RULE"], "") || strings.EqualFold(rowFKCol["UPDATE_RULE"], "NO ACTION") || strings.EqualFold(rowFKCol["UPDATE_RULE"], "RESTRICT") {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES `%s`.%s (%s) ON UPDATE %s",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList,
					strings.ToUpper(rowFKCol["UPDATE_RULE"]),
				)
				foreignKeys = append(foreignKeys, fk)
			}
			if strings.EqualFold(rowFKCol["UPDATE_RULE"], "CASCADE") {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON UPDATE CASCADE",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
				foreignKeys = append(foreignKeys, fk)
			}
			if strings.EqualFold(rowFKCol["UPDATE_RULE"], "SET NULL") {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON UPDATE SET NULL",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
				foreignKeys = append(foreignKeys, fk)
			}
		}
	}
	return foreignKeys, nil
}

func (r *Rule) GenTableCheckKey() (checkKeys []string, err error) {
	if len(r.CheckKeyINFO) > 0 {
		for _, rowFKCol := range r.CheckKeyINFO {
			searchCond := rowFKCol["SEARCH_CONDITION"]
			constraintName := rowFKCol["CONSTRAINT_NAME"]

			// 匹配替换
			for _, rowCol := range r.TableColumnINFO {
				replaceRex, err := regexp.Compile(fmt.Sprintf("(?i)%v", rowCol["COLUMN_NAME"]))
				if err != nil {
					return checkKeys, err
				}
				if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
					searchCond = replaceRex.ReplaceAllString(searchCond, strings.ToLower(rowCol["COLUMN_NAME"]))
				}
				if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
					searchCond = replaceRex.ReplaceAllString(searchCond, strings.ToUpper(rowCol["COLUMN_NAME"]))
				}
				if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
					searchCond = replaceRex.ReplaceAllString(searchCond, rowCol["COLUMN_NAME"])
				}
			}
			ck := fmt.Sprintf("CONSTRAINT %s CHECK (%s)",
				constraintName,
				searchCond)
			checkKeys = append(checkKeys, ck)
		}
	}
	return checkKeys, nil
}

func (r *Rule) GenTableUniqueIndex() (uniqueIndexes []string, compatibilityIndexSQL []string, err error) {
	// MySQL Unique Index = Unique Constraint
	return
}

func (r *Rule) GenTableNormalIndex() (normalIndexes []string, compatibilityIndexSQL []string, err error) {
	if len(r.NormalIndexINFO) > 0 {
		for _, kv := range r.NormalIndexINFO {
			var idx, columnList string
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
				columnList = strings.ToLower(kv["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
				columnList = strings.ToUpper(kv["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
				columnList = kv["COLUMN_LIST"]
			}
			if strings.EqualFold(kv["UNIQUENESS"], "UNIQUE") {
				idx = fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					r.GenSchemaName(),
					r.GenTableName(),
					columnList)
			} else {
				idx = fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					r.GenSchemaName(),
					r.GenTableName(),
					columnList)
			}
			normalIndexes = append(normalIndexes, idx)
		}
	}
	return normalIndexes, compatibilityIndexSQL, nil
}

func (r *Rule) GenTableComment() (tableComment string, err error) {
	if len(r.TableColumnINFO) > 0 && r.TableCommentINFO[0]["TABLE_COMMENT"] != "" {
		convertUtf8Raw, err := common.CharsetConvert([]byte(r.TableCommentINFO[0]["TABLE_COMMENT"]), common.MigrateStringDataTypeDatabaseCharsetMap[common.TaskTypeMySQL2Oracle][common.StringUPPER(r.SourceDBCharset)], common.MYSQLCharsetUTF8MB4)
		if err != nil {
			return tableComment, fmt.Errorf("table comments [%s] charset convert failed, %v", r.TableCommentINFO[0]["TABLE_COMMENT"], err)
		}

		convertTargetRaw, err := common.CharsetConvert([]byte(common.SpecialLettersUsingMySQL(convertUtf8Raw)), common.MYSQLCharsetUTF8MB4, common.StringUPPER(r.TargetDBCharset))
		if err != nil {
			return tableComment, fmt.Errorf("table comments [%s] charset convert failed, %v", r.TableCommentINFO[0]["TABLE_COMMENT"], err)
		}

		tableComment = fmt.Sprintf(`COMMENT ON TABLE %s.%s IS '%s';`, r.GenSchemaName(), r.GenTableName(), string(convertTargetRaw))
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
		if columnCollationMapVal, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeMySQL2Oracle][strings.ToUpper(rowCol["COLLATION_NAME"])][common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeMySQL2Oracle][strings.ToUpper(rowCol["CHARACTER_SET_NAME"])]]; ok {
			// oracle 12.2 版本及以上，字符集 columnCollation 开启需激活 extended 特性
			// columnCollation BINARY_CS : Both case and accent sensitive. This is default if no extension is used.
			if common.VersionOrdinal(r.OracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
				columnCollationArr := strings.Split(columnCollationMapVal, "/")
				if r.OracleExtendedMode {
					columnCollation = columnCollationArr[0]
				} else {
					// ORACLE 12.2 版本及以上非 extended 模式不支持设置字段 columnCollation
					// columncolumnCollation = columnCollationArr[1]
					columnCollation = ""
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

		fromSource, okFromSource := r.TableColumnDefaultValSourceRule[columnName]
		defaultVal, okDefaultVal := r.TableColumnDefaultValRule[columnName]
		if !okFromSource || !okDefaultVal {
			return columnMetas, fmt.Errorf("mysql table [%s.%s] column [%s] default value isn't exist or default value from source panic", r.SourceSchemaName, r.SourceTableName, columnName)
		}

		if fromSource {
			isTrunc := false
			if strings.HasPrefix(defaultVal, "'") && strings.HasSuffix(defaultVal, "'") {
				isTrunc = true
				defaultVal = defaultVal[1 : len(defaultVal)-1]
			}
			convertUtf8Raw, err := common.CharsetConvert([]byte(defaultVal), common.MigrateStringDataTypeDatabaseCharsetMap[common.TaskTypeMySQL2Oracle][common.StringUPPER(r.SourceDBCharset)], common.MYSQLCharsetUTF8MB4)
			if err != nil {
				return columnMetas, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
			}

			convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.MYSQLCharsetUTF8MB4, common.StringUPPER(r.TargetDBCharset))
			if err != nil {
				return columnMetas, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
			}

			if isTrunc {
				dataDefault = "'" + string(convertTargetRaw) + "'"
			} else {
				if strings.EqualFold(string(convertTargetRaw), common.OracleNULLSTRINGTableAttrWithCustom) {
					dataDefault = "'" + string(convertTargetRaw) + "'"
				} else {
					dataDefault = string(convertTargetRaw)
				}
			}
		} else {
			isTrunc := false
			if strings.HasPrefix(defaultVal, "'") && strings.HasSuffix(defaultVal, "'") {
				isTrunc = true
				defaultVal = defaultVal[1 : len(defaultVal)-1]
			}
			// meta database data utf8mb4
			convertUtf8Raw, err := common.CharsetConvert([]byte(defaultVal), common.MYSQLCharsetUTF8MB4, common.MYSQLCharsetUTF8MB4)
			if err != nil {
				return columnMetas, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
			}

			convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.MYSQLCharsetUTF8MB4, common.StringUPPER(r.TargetDBCharset))
			if err != nil {
				return columnMetas, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
			}

			if isTrunc {
				dataDefault = "'" + string(convertTargetRaw) + "'"
			} else {
				if strings.EqualFold(string(convertTargetRaw), common.OracleNULLSTRINGTableAttrWithCustom) {
					dataDefault = "'" + string(convertTargetRaw) + "'"
				} else {
					dataDefault = string(convertTargetRaw)
				}
			}
		}

		if val, ok := r.TableColumnDatatypeRule[columnName]; ok {
			columnType = val
		} else {
			return columnMetas, fmt.Errorf("mysql table [%s.%s] column [%s] data type isn't exist", r.SourceSchemaName, r.SourceTableName, columnName)
		}

		// 字段名大小写
		if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
			columnName = strings.ToLower(columnName)
		}
		if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
			columnName = strings.ToUpper(columnName)
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
				if strings.EqualFold(dataDefault, "NULL") {
					columnMetas = append(columnMetas, fmt.Sprintf("%s %s COLLATE %s %s", columnName, columnType, columnCollation, nullable))
				} else {
					columnMetas = append(columnMetas, fmt.Sprintf("%s %s COLLATE %s DEFAULT %s %s", columnName, columnType, columnCollation, dataDefault, nullable))
				}
			case columnCollation != "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("%s %s COLLATE %s %s", columnName, columnType, columnCollation, nullable))
			case columnCollation == "" && dataDefault != "":
				if strings.EqualFold(dataDefault, "NULL") {
					columnMetas = append(columnMetas, fmt.Sprintf("%s %s %s", columnName, columnType, nullable))
				} else {
					columnMetas = append(columnMetas, fmt.Sprintf("%s %s DEFAULT %s %s", columnName, columnType, dataDefault, nullable))
				}
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
			if !strings.EqualFold(rowCol["COMMENTS"], "") {
				convertUtf8Raw, err := common.CharsetConvert([]byte(rowCol["COMMENTS"]), common.MigrateStringDataTypeDatabaseCharsetMap[common.TaskTypeMySQL2Oracle][common.StringUPPER(r.SourceDBCharset)], common.MYSQLCharsetUTF8MB4)
				if err != nil {
					return nil, fmt.Errorf("column [%s] charset convert failed, %v", rowCol["COLUMN_NAME"], err)
				}

				convertTargetRaw, err := common.CharsetConvert([]byte(common.SpecialLettersUsingMySQL(convertUtf8Raw)), common.MYSQLCharsetUTF8MB4, common.StringUPPER(r.TargetDBCharset))
				if err != nil {
					return nil, fmt.Errorf("column [%s] charset convert failed, %v", rowCol["COLUMN_NAME"], err)
				}

				// 字段名大小写
				columnName := rowCol["COLUMN_NAME"]
				if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
					columnName = strings.ToLower(columnName)
				}
				if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
					columnName = strings.ToUpper(columnName)
				}

				columnComments = append(columnComments, fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS '%s';`, r.GenSchemaName(), r.GenTableName(), columnName, string(convertTargetRaw)))

			}
		}
	}
	return columnComments, nil
}

func (r *Rule) GenTablePrefix() (string, string) {
	targetSchema := r.GenSchemaName()
	targetTable := r.GenTableName()

	return targetSchema, targetTable
}

func (r *Rule) GenTableSuffix() (string, error) {
	// m2o null
	return "", nil
}

func (r *Rule) GenSchemaName() string {
	var sourceSchema, targetSchema string
	if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
		sourceSchema = strings.ToLower(r.SourceSchemaName)
		targetSchema = strings.ToLower(r.TargetSchemaName)
	}

	if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
		sourceSchema = strings.ToUpper(r.SourceSchemaName)
		targetSchema = strings.ToUpper(r.TargetSchemaName)
	}

	if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
		sourceSchema = r.SourceSchemaName
		targetSchema = r.TargetSchemaName
	}

	if targetSchema == "" {
		return sourceSchema
	}
	if targetSchema != "" {
		return targetSchema
	}
	return sourceSchema
}

func (r *Rule) GenTableName() string {
	var sourceTable, targetTable string

	if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
		sourceTable = strings.ToLower(r.SourceTableName)
		targetTable = strings.ToLower(r.TargetTableName)
	}
	if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
		sourceTable = strings.ToUpper(r.SourceTableName)
		targetTable = strings.ToUpper(r.TargetTableName)
	}
	if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
		sourceTable = r.SourceTableName
		targetTable = r.TargetTableName
	}

	if targetTable == "" {
		return sourceTable
	}
	if targetTable != "" {
		return targetTable
	}
	return sourceTable
}

func (r *Rule) String() string {
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr)
}
