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
package t2o

import (
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
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

	tablePrefix := fmt.Sprintf("CREATE TABLE %s.%s", r.TargetSchemaName, r.TargetTableName)

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
			var uk string
			if rowUKCol["CONSTRAINT_TYPE"] == "PK" {
				uk = fmt.Sprintf("PRIMARY KEY (%s)", strings.ToUpper(rowUKCol["COLUMN_LIST"]))
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
			var uk string
			if rowUKCol["CONSTRAINT_TYPE"] == "PK" {
				return uniqueKeys, fmt.Errorf("table json [%v], error on get table primary key: %v", r.String(), err)
			} else {
				uk = fmt.Sprintf("CONSTRAINT %s UNIQUE (%s)",
					strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(rowUKCol["COLUMN_LIST"]))
			}
			uniqueKeys = append(uniqueKeys, uk)
		}
	}
	return uniqueKeys, nil
}

func (r *Rule) GenTableForeignKey() (foreignKeys []string, err error) {
	if len(r.ForeignKeyINFO) > 0 {
		for _, rowFKCol := range r.ForeignKeyINFO {
			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" || rowFKCol["DELETE_RULE"] == "RESTRICT" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON DELETE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON DELETE SET NULL",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeys = append(foreignKeys, fk)
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
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["UPDATE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON UPDATE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["UPDATE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON UPDATE SET NULL",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeys = append(foreignKeys, fk)
			}
		}
	}
	return foreignKeys, nil
}

func (r *Rule) GenTableCheckKey() (checkKeys []string, err error) {
	if len(r.CheckKeyINFO) > 0 {
		for _, rowFKCol := range r.CheckKeyINFO {
			ck := fmt.Sprintf("CONSTRAINT %s CHECK (%s)",
				strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
				strings.ToUpper(rowFKCol["SEARCH_CONDITION"]))
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
			var idx string
			if kv["UNIQUENESS"] == "UNIQUE" {
				idx = fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					r.TargetSchemaName,
					r.TargetTableName,
					strings.ToUpper(kv["COLUMN_LIST"]),
				)
			} else {
				idx = fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					r.TargetSchemaName,
					r.TargetTableName,
					strings.ToUpper(kv["COLUMN_LIST"]),
				)
			}
			normalIndexes = append(normalIndexes, idx)
		}
	}
	return normalIndexes, compatibilityIndexSQL, nil
}

func (r *Rule) GenTableComment() (tableComment string, err error) {
	if len(r.TableColumnINFO) > 0 && r.TableCommentINFO[0]["TABLE_COMMENT"] != "" {
		convertUtf8Raw, err := common.CharsetConvert([]byte(r.TableCommentINFO[0]["TABLE_COMMENT"]), common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][common.StringUPPER(r.SourceDBCharset)], common.MYSQLCharsetUTF8MB4)
		if err != nil {
			return tableComment, fmt.Errorf("column [%s] charset convert failed, %v", r.TableCommentINFO[0]["TABLE_COMMENT"], err)
		}

		convertTargetRaw, err := common.CharsetConvert([]byte(common.SpecialLettersUsingMySQL(convertUtf8Raw)), common.MYSQLCharsetUTF8MB4, common.StringUPPER(r.TargetDBCharset))
		if err != nil {
			return tableComment, fmt.Errorf("column [%s] charset convert failed, %v", r.TableCommentINFO[0]["TABLE_COMMENT"], err)
		}

		tableComment = fmt.Sprintf(`COMMENT ON TABLE %s.%s IS '%s';`, r.TargetSchemaName, r.TargetTableName, string(convertTargetRaw))
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
		if columnCollationMapVal, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["COLLATION_NAME"])][common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][strings.ToUpper(rowCol["CHARACTER_SET_NAME"])]]; ok {
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
				return columnMetas, fmt.Errorf("tidb table column meta generate failed, column [%s] not support column collation [%s]", rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
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

		if val, ok := r.TableColumnDefaultValRule[columnName]; ok {
			dataDefault = val
		} else {
			return columnMetas, fmt.Errorf("mysql table [%s.%s] column [%s] default value isn't exist", r.SourceSchemaName, r.SourceTableName, columnName)
		}

		if val, ok := r.TableColumnDatatypeRule[columnName]; ok {
			columnType = val
		} else {
			return columnMetas, fmt.Errorf("mysql table [%s.%s] column [%s] data type isn't exist", r.SourceSchemaName, r.SourceTableName, columnName)
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
				convertUtf8Raw, err := common.CharsetConvert([]byte(rowCol["COMMENTS"]), common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeTiDB2Oracle][common.StringUPPER(r.SourceDBCharset)], common.MYSQLCharsetUTF8MB4)
				if err != nil {
					return nil, fmt.Errorf("column [%s] charset convert failed, %v", rowCol["COLUMN_NAME"], err)
				}

				convertTargetRaw, err := common.CharsetConvert([]byte(common.SpecialLettersUsingMySQL(convertUtf8Raw)), common.MYSQLCharsetUTF8MB4, common.StringUPPER(r.TargetDBCharset))
				if err != nil {
					return nil, fmt.Errorf("column [%s] charset convert failed, %v", rowCol["COLUMN_NAME"], err)
				}

				columnComments = append(columnComments, fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS '%s';`, r.TargetSchemaName, r.TargetTableName, rowCol["COLUMN_NAME"], string(convertTargetRaw)))

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
	if r.TargetSchemaName == "" {
		return r.SourceSchemaName
	}
	if r.TargetSchemaName != "" {
		return r.TargetSchemaName
	}
	return r.SourceSchemaName
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
