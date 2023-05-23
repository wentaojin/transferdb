/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed r. in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package o2t

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fastjson"
	"github.com/wentaojin/transferdb/common"
	"go.uber.org/zap"
	"regexp"
	"strings"
)

type Rule struct {
	*Table
	*Info
}

type Info struct {
	SourceTableDDL    string              `json:"-"` // 忽略
	PrimaryKeyINFO    []map[string]string `json:"primary_key_info"`
	UniqueKeyINFO     []map[string]string `json:"unique_key_info"`
	ForeignKeyINFO    []map[string]string `json:"foreign_key_info"`
	CheckKeyINFO      []map[string]string `json:"check_key_info"`
	UniqueIndexINFO   []map[string]string `json:"unique_index_info"`
	NormalIndexINFO   []map[string]string `json:"normal_index_info"`
	TableCommentINFO  []map[string]string `json:"table_comment_info"`
	TableColumnINFO   []map[string]string `json:"table_column_info"`
	ColumnCommentINFO []map[string]string `json:"column_comment_info"`
}

func (r *Rule) GenCreateTableDDL() (interface{}, error) {
	var (
		tablePrefix, tableComment                        string
		tableKeys, checkKeys, foreignKeys, compatibleDDL []string
	)
	targetSchema, targetTable := r.GenTablePrefix()

	tableSuffix, err := r.GenTableSuffix()
	if err != nil {
		return nil, err
	}

	tableColumns, err := r.GenTableColumn()
	if err != nil {
		return nil, err
	}

	tableKeys, compatibleDDL, err = r.GenTableKeys()
	if err != nil {
		return nil, err
	}

	tablePrefix = fmt.Sprintf("CREATE TABLE `%s`.`%s`", targetSchema, targetTable)

	checkKeys, err = r.GenTableCheckKey()
	if err != nil {
		return nil, err
	}

	foreignKeys, err = r.GenTableForeignKey()
	if err != nil {
		return nil, err
	}

	tableComment, err = r.GenTableComment()
	if err != nil {
		return nil, err
	}

	return &DDL{
		SourceSchemaName:   r.SourceSchemaName,
		SourceTableName:    r.SourceTableName,
		SourceTableType:    r.SourceTableType,
		SourceTableDDL:     r.SourceTableDDL,
		TargetSchemaName:   r.GenSchemaName(), // change schema name
		TargetTableName:    r.GenTableName(),  // change table name
		TargetDBVersion:    r.TargetDBVersion,
		TablePrefix:        tablePrefix,
		TableColumns:       tableColumns,
		TableKeys:          tableKeys,
		TableSuffix:        tableSuffix,
		TableComment:       tableComment,
		TableCheckKeys:     checkKeys,
		TableForeignKeys:   foreignKeys,
		TableCompatibleDDL: compatibleDDL,
	}, nil
}

func (r *Rule) GenTableKeys() (tableKeys []string, compatibilityIndexSQL []string, err error) {
	// 唯一约束/普通索引/唯一索引
	uniqueKeyMetas, err := r.GenTableUniqueKey()
	if err != nil {
		return tableKeys, compatibilityIndexSQL, fmt.Errorf("table json [%v], oracle db reverse table unique constraint failed: %v", r.String(), err)
	}

	normalIndexes, normalIndexCompSQL, err := r.GenTableNormalIndex()
	if err != nil {
		return tableKeys, compatibilityIndexSQL, fmt.Errorf("table json [%v], oracle db reverse table key non-unique index failed: %v", r.String(), err)
	}

	uniqueIndexes, uniqueIndexCompSQL, err := r.GenTableUniqueIndex()
	if err != nil {
		return tableKeys, compatibilityIndexSQL, fmt.Errorf("table json [%v], oracle db reverse table key unique index failed: %v", r.String(), err)
	}

	if len(normalIndexCompSQL) > 0 {
		compatibilityIndexSQL = append(compatibilityIndexSQL, normalIndexCompSQL...)
	}
	if len(uniqueIndexCompSQL) > 0 {
		compatibilityIndexSQL = append(compatibilityIndexSQL, uniqueIndexCompSQL...)
	}

	// 主键
	primaryKeys, err := r.GenTablePrimaryKey()
	if err != nil {
		return tableKeys, compatibilityIndexSQL, err
	}

	if len(primaryKeys) > 0 {
		tableKeys = append(tableKeys, primaryKeys...)
	}

	if len(uniqueKeyMetas) > 0 {
		tableKeys = append(tableKeys, uniqueKeyMetas...)
	}

	if len(uniqueIndexes) > 0 {
		tableKeys = append(tableKeys, uniqueIndexes...)
	}

	if len(normalIndexes) > 0 {
		tableKeys = append(tableKeys, normalIndexes...)
	}

	return tableKeys, compatibilityIndexSQL, nil
}

func (r *Rule) GenTablePrefix() (string, string) {
	targetSchema := r.GenSchemaName()
	targetTable := r.GenTableName()

	return targetSchema, targetTable
}

// O2M Special
func (r *Rule) GenTableSuffix() (string, error) {
	var (
		tableSuffix    string
		tableCharset   string
		tableCollation string
		primaryColumns []string
		columnMetas    []string
	)
	singleIntegerPK := false

	if len(r.PrimaryKeyINFO) > 0 {
		for _, col := range strings.Split(r.PrimaryKeyINFO[0]["COLUMN_LIST"], ",") {
			primaryColumns = append(primaryColumns, fmt.Sprintf("`%s`", col))
		}
	}

	columnMetas, err := r.GenTableColumn()
	if err != nil {
		return "", err
	}

	// 单列主键且整型主键
	if len(primaryColumns) == 1 {
		// 单列主键数据类型获取判断
		for _, columnMeta := range columnMetas {
			columnName := strings.Fields(columnMeta)[0]
			columnType := strings.Fields(columnMeta)[1]

			if strings.EqualFold(primaryColumns[0], columnName) {
				// Map 规则转换后的字段对应数据类型
				// columnMeta 视角 columnName columnType ....
				for _, integerType := range common.TiDBIntegerPrimaryKeyList {
					if find := strings.Contains(common.StringUPPER(columnType), common.StringUPPER(integerType)); find {
						singleIntegerPK = true
					}
				}
			}
		}
	}

	tableCharset = common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeOracle2TiDB][r.SourceDBCharset]

	// schema、db、table collation
	if r.OracleCollation {
		// table collation
		if r.SourceTableCollation != "" {
			if val, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeOracle2TiDB][r.SourceTableCollation][tableCharset]; ok {
				tableCollation = val
			} else {
				return tableSuffix, fmt.Errorf("oracle table collation [%v] isn't support", r.SourceTableCollation)
			}
		}
		// schema collation
		if r.SourceTableCollation == "" && r.SourceSchemaCollation != "" {
			if val, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeOracle2TiDB][r.SourceSchemaCollation][tableCharset]; ok {
				tableCollation = val
			} else {
				return tableSuffix, fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support", r.SourceSchemaCollation, r.SourceTableCollation)
			}
		}
		if r.SourceTableName == "" && r.SourceSchemaCollation == "" {
			return tableSuffix, fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support", r.SourceSchemaCollation, r.SourceTableCollation)
		}
	} else {
		// db collation
		if val, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeOracle2TiDB][r.SourceDBNLSComp][tableCharset]; ok {
			tableCollation = val
		} else {
			return tableSuffix, fmt.Errorf("oracle db nls_comp [%v] nls_sort [%v] isn't support", r.SourceDBNLSComp, r.SourceDBNLSSort)
		}
	}

	// table-option 表后缀可选项
	if r.TargetTableOption == "" {
		zap.L().Warn("reverse oracle table suffix",
			zap.String("table", r.String()),
			zap.String("table-option", "table-option is null, would be disabled"))
		// table suffix
		tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
			strings.ToLower(tableCharset), strings.ToLower(tableCollation))

	} else {
		// TiDB
		clusteredIdxVal, err := r.MySQL.GetTiDBClusteredIndexValue()
		if err != nil {
			return tableSuffix, err
		}
		switch common.StringUPPER(clusteredIdxVal) {
		case common.TiDBClusteredIndexOFFValue:
			zap.L().Warn("reverse oracle table suffix",
				zap.String("table", r.String()),
				zap.String("tidb_enable_clustered_index", common.TiDBClusteredIndexOFFValue),
				zap.String("table-option", "tidb_enable_clustered_index is off, would be enabled"))

			if r.TargetTableOption != "" {
				tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s %s",
					strings.ToLower(tableCharset), strings.ToLower(tableCollation), common.StringUPPER(r.TargetTableOption))
			} else {
				tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
					strings.ToLower(tableCharset), strings.ToLower(tableCollation))
			}
		case common.TiDBClusteredIndexONValue:
			zap.L().Warn("reverse oracle table suffix",
				zap.String("table", r.String()),
				zap.String("tidb_enable_clustered_index", common.TiDBClusteredIndexONValue),
				zap.String("table-option", "tidb_enable_clustered_index is on, would be disabled"))

			tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
				strings.ToLower(tableCharset), strings.ToLower(tableCollation))

		default:
			// tidb_enable_clustered_index = int_only / tidb_enable_clustered_index 不存在值，等于空
			pkVal, err := r.MySQL.GetTiDBAlterPKValue()
			if err != nil {
				return tableSuffix, err
			}
			if !fastjson.Exists([]byte(pkVal), "alter-primary-key") {
				zap.L().Warn("reverse oracle table suffix",
					zap.String("table", r.String()),
					zap.String("tidb_enable_clustered_index", common.StringUPPER(clusteredIdxVal)),
					zap.String("alter-primary-key", "not exist"),
					zap.String("table-option", "alter-primary-key isn't exits, would be disable"))

				tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
					strings.ToLower(tableCharset), strings.ToLower(tableCollation))

			} else {
				var p fastjson.Parser
				v, err := p.Parse(pkVal)
				if err != nil {
					return tableSuffix, err
				}

				isAlterPK := v.GetBool("alter-primary-key")

				// alter-primary-key = false
				// 整型主键 table-option 不生效
				// 单列主键是整型
				if !isAlterPK && len(primaryColumns) == 1 && singleIntegerPK {
					zap.L().Warn("reverse oracle table suffix",
						zap.String("table", r.String()),
						zap.String("tidb_enable_clustered_index", common.StringUPPER(clusteredIdxVal)),
						zap.Bool("alter-primary-key", isAlterPK),
						zap.String("table-option", "integer primary key, would be disable"))

					tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
						strings.ToLower(tableCharset), strings.ToLower(tableCollation))

				} else {
					// table-option 生效
					// alter-primary-key = true
					// alter-primary-key = false && 联合主键 len(pkINFO)>1
					// alter-primary-key = false && 非整型主键
					if isAlterPK || (!isAlterPK && len(primaryColumns) > 1) || (!isAlterPK && !singleIntegerPK) {
						zap.L().Warn("reverse oracle table suffix",
							zap.String("table", r.String()),
							zap.String("tidb_enable_clustered_index", common.StringUPPER(clusteredIdxVal)),
							zap.Bool("alter-primary-key", isAlterPK),
							zap.String("table-option", "enabled"))

						if r.TargetTableOption != "" {
							tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s %s",
								strings.ToLower(tableCharset), strings.ToLower(tableCollation), common.StringUPPER(r.TargetTableOption))
						} else {
							tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
								strings.ToLower(tableCharset), strings.ToLower(tableCollation))
						}
					} else {
						zap.L().Error("reverse oracle table suffix",
							zap.String("table", r.String()),
							zap.String("tidb_enable_clustered_index", common.StringUPPER(clusteredIdxVal)),
							zap.Bool("alter-primary-key", isAlterPK),
							zap.String("table-option", "disabled"),
							zap.Error(fmt.Errorf("not support")))
						return tableSuffix, fmt.Errorf("reverse oracle table suffix error: table-option not support")
					}
				}
			}
		}
	}
	zap.L().Info("reverse oracle table suffix",
		zap.String("table", r.String()),
		zap.String("create table suffix", tableSuffix))

	return tableSuffix, nil
}

func (r *Rule) GenTablePrimaryKey() (primaryKeys []string, err error) {
	if len(r.PrimaryKeyINFO) > 1 {
		return primaryKeys, fmt.Errorf("oracle schema [%s] table [%s] primary key exist multiple values: [%v]", r.SourceSchemaName, r.SourceTableName, r.PrimaryKeyINFO)
	}
	if len(r.PrimaryKeyINFO) > 0 {
		var primaryColumns []string
		for _, col := range strings.Split(r.PrimaryKeyINFO[0]["COLUMN_LIST"], ",") {
			primaryColumns = append(primaryColumns, fmt.Sprintf("`%s`", col))
		}
		pk := fmt.Sprintf("PRIMARY KEY (%s)", strings.ToUpper(strings.Join(primaryColumns, ",")))
		primaryKeys = append(primaryKeys, pk)
	}

	return primaryKeys, nil
}

func (r *Rule) GenTableUniqueKey() (uniqueKeys []string, err error) {
	if len(r.UniqueKeyINFO) > 0 {
		for _, rowUKCol := range r.UniqueKeyINFO {
			var ukArr []string
			for _, col := range strings.Split(rowUKCol["COLUMN_LIST"], ",") {
				ukArr = append(ukArr, fmt.Sprintf("`%s`", col))
			}
			uk := fmt.Sprintf("UNIQUE KEY `%s` (%s)",
				strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(strings.Join(ukArr, ",")))

			uniqueKeys = append(uniqueKeys, uk)
		}
	}
	return uniqueKeys, nil
}

func (r *Rule) GenTableForeignKey() (foreignKeys []string, err error) {
	if len(r.ForeignKeyINFO) > 0 {
		for _, rowFKCol := range r.ForeignKeyINFO {
			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s)",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s`(%s) ON DELETE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				foreignKeys = append(foreignKeys, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE SET NULL",
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
		// 多个检查约束匹配
		// 比如："LOC" IS noT nUll and loc in ('a','b','c')
		reg, err := regexp.Compile(`\s+(?i:AND)\s+|\s+(?i:OR)\s+`)
		if err != nil {
			return checkKeys, fmt.Errorf("check constraint regexp [AND/OR] failed: %v", err)
		}

		matchRex, err := regexp.Compile(`(^.*)(?i:IS NOT NULL)`)
		if err != nil {
			return checkKeys, fmt.Errorf("check constraint regexp match [IS NOT NULL] failed: %v", err)
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
					checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
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

				checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
					strings.ToUpper(rowCKCol["CONSTRAINT_NAME"]),
					strings.Join(d, " ")))
			}
		}
	}

	return checkKeys, nil
}

func (r *Rule) GenTableUniqueIndex() (uniqueIndexes []string, compatibilityIndexSQL []string, err error) {
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

					uniqueIndexes = append(uniqueIndexes, uniqueIDX)

					zap.L().Info("reverse unique index",
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("unique index info", uniqueIDX))

					continue

				case "FUNCTION-BASED NORMAL":
					sql := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchemaName, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse unique key",
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create unique index sql", sql),
						zap.String("warn", "mysql not support"))

					continue

				default:
					zap.L().Error("reverse unique index",
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("error", "mysql not support"))

					return uniqueIndexes, compatibilityIndexSQL, fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] reverse normal index panic, error: %v", r.SourceSchemaName, r.SourceTableName, idxMeta)
				}
			}
			zap.L().Error("reverse unique key",
				zap.String("schema", r.SourceSchemaName),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]))
			return uniqueIndexes, compatibilityIndexSQL,
				fmt.Errorf("[NON-UNIQUE] oracle schema [%s] table [%s] panic, error: %v", r.SourceSchemaName, r.SourceTableName, idxMeta)
		}
	}

	return uniqueIndexes, compatibilityIndexSQL, err
}

func (r *Rule) GenTableNormalIndex() (normalIndexes []string, compatibilityIndexSQL []string, err error) {
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

					normalIndexes = append(normalIndexes, keyIndex)

					zap.L().Info("reverse normal index",
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("key index info", keyIndex))

					continue

				case "FUNCTION-BASED NORMAL":
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchemaName, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "BITMAP":
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchemaName, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "FUNCTION-BASED BITMAP":
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchemaName, r.TargetTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "DOMAIN":
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');",
						strings.ToUpper(idxMeta["INDEX_NAME"]), r.TargetSchemaName, r.TargetTableName,
						idxMeta["COLUMN_LIST"],
						strings.ToUpper(idxMeta["ITYP_OWNER"]),
						strings.ToUpper(idxMeta["ITYP_NAME"]),
						idxMeta["PARAMETERS"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					zap.L().Warn("reverse normal index",
						zap.String("schema", r.SourceSchemaName),
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
						zap.String("schema", r.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("domain owner", idxMeta["ITYP_OWNER"]),
						zap.String("domain index name", idxMeta["ITYP_NAME"]),
						zap.String("domain parameters", idxMeta["PARAMETERS"]),
						zap.String("error", "mysql not support"))

					return normalIndexes, compatibilityIndexSQL, fmt.Errorf("[NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", r.SourceSchemaName, r.SourceTableName, idxMeta)
				}
			}

			zap.L().Error("reverse normal index",
				zap.String("schema", r.SourceSchemaName),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]),
				zap.String("domain owner", idxMeta["ITYP_OWNER"]),
				zap.String("domain index name", idxMeta["ITYP_NAME"]),
				zap.String("domain parameters", idxMeta["PARAMETERS"]))
			return normalIndexes, compatibilityIndexSQL, fmt.Errorf("[NON-NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", r.SourceSchemaName, r.SourceTableName, idxMeta)
		}
	}

	return normalIndexes, compatibilityIndexSQL, err
}

func (r *Rule) GenTableComment() (tableComment string, err error) {
	if len(r.TableColumnINFO) > 0 && r.TableCommentINFO[0]["COMMENTS"] != "" {
		tableComment = fmt.Sprintf("COMMENT='%s'", r.TableCommentINFO[0]["COMMENTS"])
	}
	return tableComment, err
}

func (r *Rule) GenTableColumn() (tableColumns []string, err error) {
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
			if collationMapVal, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeOracle2TiDB][strings.ToUpper(rowCol["COLLATION"])][common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeOracle2TiDB][r.SourceDBCharset]]; ok {
				columnCollation = collationMapVal
			} else {
				// 字段数值数据类型不存在排序规则，排除忽略
				if !strings.EqualFold(rowCol["COLLATION"], "") {
					return tableColumns, fmt.Errorf(`error on check oracle column [%v] collation: %v`, rowCol["COLUMN_NAME"], rowCol["COLLATION"])
				}
				columnCollation = ""
			}
		} else {
			// oracle 12.2 版本以下不支持，置空
			columnCollation = ""
		}

		if val, ok := r.TableColumnDatatypeRule[rowCol["COLUMN_NAME"]]; ok {
			columnType = val
		} else {
			return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] data type isn't exist", r.SourceSchemaName, r.SourceTableName, rowCol["COLUMN_NAME"])
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

		if val, ok := r.TableColumnDefaultValRule[rowCol["COLUMN_NAME"]]; ok {
			dataDefault = val
		} else {
			return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] default value isn't exist", r.SourceSchemaName, r.SourceTableName, rowCol["COLUMN_NAME"])
		}

		if nullable == "NULL" {
			switch {
			case columnCollation != "" && comment != "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, dataDefault, comment))
			case columnCollation != "" && comment == "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, dataDefault))
			case columnCollation != "" && comment == "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s", rowCol["COLUMN_NAME"], columnType, columnCollation))
			case columnCollation != "" && comment != "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, comment))
			case columnCollation == "" && comment != "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, dataDefault, comment))
			case columnCollation == "" && comment == "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, dataDefault))
			case columnCollation == "" && comment == "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s", rowCol["COLUMN_NAME"], columnType))
			case columnCollation == "" && comment != "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, comment))
			default:
				return tableColumns, fmt.Errorf("error on gen oracle schema table column meta with nullable, rule: %v", r.String())
			}
		} else {
			switch {
			case columnCollation != "" && comment != "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s COMMENT %s",
					rowCol["COLUMN_NAME"], columnType, columnCollation, nullable, dataDefault, comment))
			case columnCollation != "" && comment != "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, nullable, comment))
			case columnCollation != "" && comment == "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, columnCollation, nullable, dataDefault))
			case columnCollation != "" && comment == "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s", rowCol["COLUMN_NAME"], columnType, columnCollation, nullable))
			case columnCollation == "" && comment != "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, nullable, dataDefault, comment))
			case columnCollation == "" && comment != "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s COMMENT %s", rowCol["COLUMN_NAME"], columnType, nullable, comment))
			case columnCollation == "" && comment == "" && dataDefault != "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s", rowCol["COLUMN_NAME"], columnType, nullable, dataDefault))
			case columnCollation == "" && comment == "" && dataDefault == "":
				tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s", rowCol["COLUMN_NAME"], columnType, nullable))
			default:
				return tableColumns, fmt.Errorf("error on gen oracle schema table column meta without nullable, rule: %v", r.String())
			}
		}
	}

	return tableColumns, nil
}

func (r *Rule) GenTableColumnComment() (columnComments []string, err error) {
	// O2T Skip
	return
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
