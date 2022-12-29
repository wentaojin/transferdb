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
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"go.uber.org/zap"
	"regexp"
	"strings"
)

type Rule struct {
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
	OracleCollation          bool                `json:"oracle_collation"`
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
	reverseDDL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s,\n%s\n)",
		targetSchema,
		targetTable,
		strings.Join(tableColumnMetas, ",\n"),
		strings.Join(tableKeyMetas, ",\n"))

	if len(tableKeyMetas) > 0 {
		reverseDDL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s,\n%s\n)",
			targetSchema,
			targetTable,
			strings.Join(tableColumnMetas, ",\n"),
			strings.Join(tableKeyMetas, ",\n"))
	} else {
		reverseDDL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s\n)",
			targetSchema,
			targetTable,
			strings.Join(tableColumnMetas, ",\n"))
	}

	// table check、foreign -> target
	checkKeyMetas, err := r.GenTableCheckKey()
	if err != nil {
		return reverseDDL, checkKeyDDL, foreignKeyDDL, compatibleDDL, err
	}

	if len(checkKeyMetas) > 0 {
		for _, ck := range checkKeyMetas {
			ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", targetSchema, targetTable, ck)
			zap.L().Info("reverse oracle table check key",
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

	if len(foreignKeyMetas) > 0 {
		for _, fk := range foreignKeyMetas {
			addFkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", targetSchema, targetTable, fk)
			zap.L().Info("reverse oracle table foreign key",
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
		return tableKeyMetas, compatibilityIndexSQL, fmt.Errorf("table json [%v], oracle db reverse table unique constraint failed: %v", r.String(), err)
	}

	normalIndexMetas, normalIndexCompSQL, err := r.GenTableNormalIndex()
	if err != nil {
		return tableKeyMetas, compatibilityIndexSQL, fmt.Errorf("table json [%v], oracle db reverse table key non-unique index failed: %v", r.String(), err)
	}

	uniqueIndexMetas, uniqueIndexCompSQL, err := r.GenTableUniqueIndex()
	if err != nil {
		return tableKeyMetas, compatibilityIndexSQL, fmt.Errorf("table json [%v], oracle db reverse table key unique index failed: %v", r.String(), err)
	}

	if len(normalIndexCompSQL) > 0 {
		compatibilityIndexSQL = append(compatibilityIndexSQL, normalIndexCompSQL...)
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

	if len(normalIndexMetas) > 0 {
		tableKeyMetas = append(tableKeyMetas, normalIndexMetas...)
	}

	return tableKeyMetas, compatibilityIndexSQL, nil
}

func (r *Rule) GenTableNamePrefix() (string, string) {
	targetSchema := r.GenSchemaName()
	targetTable := r.GenTableName()

	return targetSchema, targetTable
}

func (r *Rule) GenTablePrimaryKey() (primaryKeyMetas []string, err error) {
	if len(r.PrimaryKeyINFO) > 1 {
		return primaryKeyMetas, fmt.Errorf("oracle schema [%s] table [%s] primary key exist multiple values: [%v]", r.SourceSchema, r.SourceTableName, r.PrimaryKeyINFO)
	}
	if len(r.PrimaryKeyINFO) > 0 {
		var primaryColumns []string
		for _, col := range strings.Split(r.PrimaryKeyINFO[0]["COLUMN_LIST"], ",") {
			primaryColumns = append(primaryColumns, fmt.Sprintf("`%s`", col))
		}
		pk := fmt.Sprintf("PRIMARY KEY (%s)", strings.ToUpper(strings.Join(primaryColumns, ",")))
		primaryKeyMetas = append(primaryKeyMetas, pk)
	}

	return primaryKeyMetas, nil
}

func (r *Rule) GenTableUniqueKey() (uniqueKeyMetas []string, err error) {
	if len(r.UniqueKeyINFO) > 0 {
		for _, rowUKCol := range r.UniqueKeyINFO {
			var ukArr []string
			for _, col := range strings.Split(rowUKCol["COLUMN_LIST"], ",") {
				ukArr = append(ukArr, fmt.Sprintf("`%s`", col))
			}
			uk := fmt.Sprintf("UNIQUE KEY `%s` (%s)",
				strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(strings.Join(ukArr, ",")))

			uniqueKeyMetas = append(uniqueKeyMetas, uk)
		}
	}
	return uniqueKeyMetas, nil
}

func (r *Rule) GenTableForeignKey() (foreignKeyMetas []string, err error) {
	if len(r.ForeignKeyINFO) > 0 {
		for _, rowFKCol := range r.ForeignKeyINFO {
			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s)",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s`(%s) ON DELETE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeyMetas = append(foreignKeyMetas, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE SET NULL",
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
		// 多个检查约束匹配
		// 比如："LOC" IS noT nUll and loc in ('a','b','c')
		reg, err := regexp.Compile(`\s+(?i:AND)\s+|\s+(?i:OR)\s+`)
		if err != nil {
			return checkKeyMetas, fmt.Errorf("check constraint regexp [AND/OR] failed: %v", err)
		}

		matchRex, err := regexp.Compile(`(^.*)(?i:IS NOT NULL)`)
		if err != nil {
			return checkKeyMetas, fmt.Errorf("check constraint regexp match [IS NOT NULL] failed: %v", err)
		}

		checkRex, err := regexp.Compile(`(.*)(?i:IS NOT NULL)`)
		if err != nil {
			fmt.Printf("check constraint regexp check [IS NOT NULL] failed: %v", err)
		}

		for _, rowCKCol := range r.CheckKeyINFO {
			// 排除非空约束检查
			s := strings.TrimSpace(rowCKCol["SEARCH_CONDITION"])

			if !reg.MatchString(s) {
				if !matchRex.MatchString(s) {
					checkKeyMetas = append(checkKeyMetas, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
						strings.ToUpper(rowCKCol["CONSTRAINT_NAME"]),
						rowCKCol["SEARCH_CONDITION"]))
				}
			} else {

				strArray := strings.Fields(s)

				var (
					idxArray        []int
					checkArray      []string
					constraintArray []string
				)
				for idx, val := range strArray {
					if strings.EqualFold(val, "AND") || strings.EqualFold(val, "OR") {
						idxArray = append(idxArray, idx)
					}
				}

				idxArray = append(idxArray, len(strArray))

				for idx, val := range idxArray {
					if idx == 0 {
						checkArray = append(checkArray, strings.Join(strArray[0:val], " "))
					} else {
						checkArray = append(checkArray, strings.Join(strArray[idxArray[idx-1]:val], " "))
					}
				}

				for _, val := range checkArray {
					v := strings.TrimSpace(val)
					if !checkRex.MatchString(v) {
						constraintArray = append(constraintArray, v)
					}
				}

				sd := strings.Join(constraintArray, " ")
				d := strings.Fields(sd)

				if strings.EqualFold(d[0], "AND") || strings.EqualFold(d[0], "OR") {
					d = d[1:]
				}
				if strings.EqualFold(d[len(d)-1], "AND") || strings.EqualFold(d[len(d)-1], "OR") {
					d = d[:len(d)-1]
				}

				checkKeyMetas = append(checkKeyMetas, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
					strings.ToUpper(rowCKCol["CONSTRAINT_NAME"]),
					strings.Join(d, " ")))
			}
		}
	}

	return checkKeyMetas, nil
}

func (r *Rule) GenTableUniqueIndex() (uniqueIndexMetas []string, compatibilityIndexSQL []string, err error) {
	if len(r.UniqueIndexINFO) > 0 {
		for _, idxMeta := range r.UniqueIndexINFO {
			if idxMeta["TABLE_NAME"] != "" && strings.ToUpper(idxMeta["UNIQUENESS"]) == "UNIQUE" {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					var uniqueIndex []string
					for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
						uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
					}

					uniqueIDX := fmt.Sprintf("UNIQUE INDEX `%s` (%s)", strings.ToUpper(idxMeta["INDEX_NAME"]), strings.Join(uniqueIndex, ","))

					uniqueIndexMetas = append(uniqueIndexMetas, uniqueIDX)

					zap.L().Info("reverse unique index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("unique index info", uniqueIDX))

					continue

				case "FUNCTION-BASED NORMAL":
					sql := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchema, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse unique key",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create unique index sql", sql),
						zap.String("warn", "mysql not support"))

					continue

				default:
					zap.L().Error("reverse unique index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("error", "mysql not support"))

					return uniqueIndexMetas, compatibilityIndexSQL, fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] reverse normal index panic, error: %v", r.SourceSchema, r.SourceTableName, idxMeta)
				}
			}
			zap.L().Error("reverse unique key",
				zap.String("schema", r.SourceSchema),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]))
			return uniqueIndexMetas, compatibilityIndexSQL,
				fmt.Errorf("[NON-UNIQUE] oracle schema [%s] table [%s] panic, error: %v", r.SourceSchema, r.SourceTableName, idxMeta)
		}
	}

	return uniqueIndexMetas, compatibilityIndexSQL, err
}

func (r *Rule) GenTableNormalIndex() (normalIndexMetas []string, compatibilityIndexSQL []string, err error) {
	// 普通索引【普通索引、函数索引、位图索引、DOMAIN 索引】
	if len(r.NormalIndexINFO) > 0 {
		for _, idxMeta := range r.NormalIndexINFO {
			if idxMeta["TABLE_NAME"] != "" && strings.ToUpper(idxMeta["UNIQUENESS"]) == "NONUNIQUE" {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					var normalIndex []string
					for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
						normalIndex = append(normalIndex, fmt.Sprintf("`%s`", col))
					}

					keyIndex := fmt.Sprintf("KEY `%s` (%s)", strings.ToUpper(idxMeta["INDEX_NAME"]), strings.Join(normalIndex, ","))

					normalIndexMetas = append(normalIndexMetas, keyIndex)

					zap.L().Info("reverse normal index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("key index info", keyIndex))

					continue

				case "FUNCTION-BASED NORMAL":
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchema, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "BITMAP":
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchema, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "FUNCTION-BASED BITMAP":
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchema, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "DOMAIN":
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchema, r.TargetTableName,
						idxMeta["COLUMN_LIST"],
						strings.ToUpper(idxMeta["ITYP_OWNER"]),
						strings.ToUpper(idxMeta["ITYP_NAME"]),
						idxMeta["PARAMETERS"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("domain owner", idxMeta["ITYP_OWNER"]),
						zap.String("domain index name", idxMeta["ITYP_NAME"]),
						zap.String("domain parameters", idxMeta["PARAMETERS"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				default:
					zap.L().Error("reverse normal index",
						zap.String("schema", r.SourceSchema),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("domain owner", idxMeta["ITYP_OWNER"]),
						zap.String("domain index name", idxMeta["ITYP_NAME"]),
						zap.String("domain parameters", idxMeta["PARAMETERS"]),
						zap.String("error", "mysql not support"))

					return normalIndexMetas, compatibilityIndexSQL, fmt.Errorf("[NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", r.SourceSchema, r.SourceTableName, idxMeta)
				}
			}

			zap.L().Error("reverse normal index",
				zap.String("schema", r.SourceSchema),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]),
				zap.String("domain owner", idxMeta["ITYP_OWNER"]),
				zap.String("domain index name", idxMeta["ITYP_NAME"]),
				zap.String("domain parameters", idxMeta["PARAMETERS"]))
			return normalIndexMetas, compatibilityIndexSQL, fmt.Errorf("[NON-NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", r.SourceSchema, r.SourceTableName, idxMeta)
		}
	}

	return normalIndexMetas, compatibilityIndexSQL, err
}

func (r *Rule) GenTableComment() (tableComment string, err error) {
	if len(r.TableColumnINFO) > 0 && r.TableCommentINFO[0]["COMMENTS"] != "" {
		tableComment = fmt.Sprintf("COMMENT='%s'", r.TableCommentINFO[0]["COMMENTS"])
	}
	return tableComment, err
}

func (r *Rule) GenTableColumn() (columnMetas []string, err error) {
	for _, rowCol := range r.TableColumnINFO {
		var (
			columnCollation string
			nullable        string
			comment         string
			dataDefault     string
			columnType      string
		)
		if r.OracleCollation {
			// 字段排序规则检查
			if collationMapVal, ok := common.OracleCollationMap[strings.ToUpper(rowCol["COLLATION"])]; ok {
				columnCollation = collationMapVal
			} else {
				// 字段数值数据类型不存在排序规则，排除忽略
				if !strings.EqualFold(rowCol["COLLATION"], "") {
					return columnMetas, fmt.Errorf(`error on check oracle column [%v] collation: %v`, rowCol["COLUMN_NAME"], rowCol["COLLATION"])
				}
				columnCollation = ""
			}
		} else {
			// oracle 12.2 版本以下不支持，置空
			columnCollation = ""
		}

		if val, ok := r.ColumnDatatypeRule[rowCol["COLUMN_NAME"]]; ok {
			columnType = val
		} else {
			return columnMetas, fmt.Errorf("oracle table [%s.%s] column [%s] data type isn't exist", r.SourceSchema, r.SourceTableName, rowCol["COLUMN_NAME"])
		}

		if strings.EqualFold(rowCol["NULLABLE"], "Y") {
			nullable = "NULL"
		} else {
			nullable = "NOT NULL"
		}

		if !strings.EqualFold(rowCol["COMMENTS"], "") {
			comment = "'" + common.SpecialLettersUsingMySQL([]byte(rowCol["COMMENTS"])) + "'"
		} else {
			comment = rowCol["COMMENTS"]
		}

		if !strings.EqualFold(rowCol["DATA_DEFAULT"], "") {
			if val, ok := r.ColumnDataDefaultvalRule[rowCol["COLUMN_NAME"]]; ok {
				dataDefault = val
			} else {
				return columnMetas, fmt.Errorf("oracle table [%s.%s] column [%s] default value isn't exist", r.SourceSchema, r.SourceTableName, rowCol["COLUMN_NAME"])
			}
		} else {
			dataDefault = rowCol["DATA_DEFAULT"]
		}

		if nullable == "NULL" {
			switch {
			case columnCollation != "" && comment != "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, dataDefault, comment))
			case columnCollation != "" && comment == "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, dataDefault))
			case columnCollation != "" && comment == "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s", rowCol["COLUMN_NAME"], columnType, columnCollation))
			case columnCollation != "" && comment != "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, comment))
			case columnCollation == "" && comment != "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, dataDefault, comment))
			case columnCollation == "" && comment == "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, dataDefault))
			case columnCollation == "" && comment == "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s", rowCol["COLUMN_NAME"], columnType))
			case columnCollation == "" && comment != "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, comment))
			default:
				return columnMetas, fmt.Errorf("error on gen oracle schema table column meta with nullable, rule: %v", r.String())
			}
		} else {
			switch {
			case columnCollation != "" && comment != "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s COMMENT %s",
					rowCol["COLUMN_NAME"], columnType, columnCollation, nullable, dataDefault, comment))
			case columnCollation != "" && comment != "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, nullable, comment))
			case columnCollation != "" && comment == "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, nullable, dataDefault))
			case columnCollation != "" && comment == "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s COLLATE %s %s", rowCol["COLUMN_NAME"], columnType, columnCollation, nullable))
			case columnCollation == "" && comment != "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, nullable, dataDefault, comment))
			case columnCollation == "" && comment != "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, nullable, comment))
			case columnCollation == "" && comment == "" && dataDefault != "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, nullable, dataDefault))
			case columnCollation == "" && comment == "" && dataDefault == "":
				columnMetas = append(columnMetas, fmt.Sprintf("`%s` %s %s", rowCol["COLUMN_NAME"], columnType, nullable))
			default:
				return columnMetas, fmt.Errorf("error on gen oracle schema table column meta without nullable, rule: %v", r.String())
			}
		}
	}

	return columnMetas, nil
}

func (r *Rule) GenTableColumnComment() (columnComments []string, err error) {
	// O2M Skip
	return
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
	// 额外处理 Oracle 默认值 ('6') 或者 (5) 或者 ('xsddd') 等包含小括号的默认值，而非 '(xxxx)' 之类的默认值
	// Oracle 对于同类型 ('xxx') 或者 (xxx) 内部会自动处理，所以 O2M/O2T 需要处理成 'xxx' 或者 xxx
	if strings.HasPrefix(defaultValue, "(") && strings.HasSuffix(defaultValue, ")") {
		defaultValue = strings.TrimLeft(defaultValue, "(")
		defaultValue = strings.TrimRight(defaultValue, ")")
	}

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
