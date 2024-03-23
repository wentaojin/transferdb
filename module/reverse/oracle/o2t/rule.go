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
	"regexp"
	"strings"

	"github.com/valyala/fastjson"
	"github.com/wentaojin/transferdb/common"
	"go.uber.org/zap"
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
		schema, table, tableComment                      string
		tableKeys, checkKeys, foreignKeys, compatibleDDL []string
	)

	schema, err := r.GenSchemaName()
	if err != nil {
		return nil, err
	}
	table, err = r.GenTableName()
	if err != nil {
		return nil, err
	}
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
		TargetSchemaName:   schema, // change schema name
		TargetTableName:    table,  // change table name
		TargetDBVersion:    r.TargetDBVersion,
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
		if r.SourceTableCollation == "" && r.SourceSchemaCollation == "" {
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

	if val, ok := r.TargetTableNonClustered[r.SourceTableName]; ok {
		tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s %s",
			tableCharset, tableCollation, common.StringUPPER(val))
		zap.L().Info("reverse oracle table suffix",
			zap.String("table", r.String()),
			zap.String("create table suffix", tableSuffix))

		return tableSuffix, nil
	}

	// clustered index table must exist primary key
	if _, ok := r.TargetTableClustered[r.SourceTableName]; ok && len(primaryColumns) != 0 {
		tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
			tableCharset, tableCollation)

		zap.L().Info("reverse oracle table suffix",
			zap.String("table", r.String()),
			zap.String("create table suffix", tableSuffix))

		return tableSuffix, nil
	}

	// table-option 表后缀可选项
	if r.TargetTableOption == "" {
		zap.L().Warn("reverse oracle table suffix",
			zap.String("table", r.String()),
			zap.String("table-option", "table-option is null, would be disabled"))
		// table suffix
		tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
			tableCharset, tableCollation)

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
					tableCharset, tableCollation, common.StringUPPER(r.TargetTableOption))
			} else {
				tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
					tableCharset, tableCollation)
			}
		case common.TiDBClusteredIndexONValue:
			zap.L().Warn("reverse oracle table suffix",
				zap.String("table", r.String()),
				zap.String("tidb_enable_clustered_index", common.TiDBClusteredIndexONValue),
				zap.String("table-option", "tidb_enable_clustered_index is on, would be disabled"))

			tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
				tableCharset, tableCollation)

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
					tableCharset, tableCollation)

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
						tableCharset, tableCollation)

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
								tableCharset, tableCollation, common.StringUPPER(r.TargetTableOption))
						} else {
							tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s",
								tableCharset, tableCollation)
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
		var (
			columnList     string
			primaryColumns []string
		)
		if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
			columnList = strings.ToLower(r.PrimaryKeyINFO[0]["COLUMN_LIST"])
		}
		if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
			columnList = strings.ToUpper(r.PrimaryKeyINFO[0]["COLUMN_LIST"])
		}
		if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
			columnList = r.PrimaryKeyINFO[0]["COLUMN_LIST"]
		}
		for _, col := range strings.Split(columnList, ",") {
			primaryColumns = append(primaryColumns, fmt.Sprintf("`%s`", col))
		}

		var pk string
		if _, ok := r.TargetTableNonClustered[r.SourceTableName]; ok {
			pk = fmt.Sprintf("PRIMARY KEY (%s) NONCLUSTERED", strings.Join(primaryColumns, ","))
			primaryKeys = append(primaryKeys, pk)
			return primaryKeys, nil
		}
		if _, ok := r.TargetTableClustered[r.SourceTableName]; ok {
			pk = fmt.Sprintf("PRIMARY KEY (%s) CLUSTERED", strings.Join(primaryColumns, ","))
			primaryKeys = append(primaryKeys, pk)
			return primaryKeys, nil
		}
		pk = fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryColumns, ","))
		primaryKeys = append(primaryKeys, pk)
		return primaryKeys, nil
	}

	return primaryKeys, nil
}

func (r *Rule) GenTableUniqueKey() (uniqueKeys []string, err error) {
	if len(r.UniqueKeyINFO) > 0 {
		for _, rowUKCol := range r.UniqueKeyINFO {
			var (
				ukArr      []string
				columnList string
			)
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
				columnList = strings.ToLower(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
				columnList = strings.ToUpper(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
				columnList = rowUKCol["COLUMN_LIST"]
			}
			for _, col := range strings.Split(columnList, ",") {
				ukArr = append(ukArr, fmt.Sprintf("`%s`", col))
			}
			uk := fmt.Sprintf("UNIQUE KEY `%s` (%s)",
				rowUKCol["CONSTRAINT_NAME"], strings.Join(ukArr, ","))
			uniqueKeys = append(uniqueKeys, uk)
		}
	}
	return uniqueKeys, nil
}

func (r *Rule) GenTableForeignKey() (foreignKeys []string, err error) {
	if len(r.ForeignKeyINFO) > 0 {
		var (
			fk          string
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

			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" {
				fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s)",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s`(%s) ON DELETE CASCADE",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE SET NULL",
					rowFKCol["CONSTRAINT_NAME"],
					columnList,
					rOwner,
					rTable,
					rColumnList)
			}
			foreignKeys = append(foreignKeys, fk)
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

			searchCond := rowCKCol["SEARCH_CONDITION"]
			constraintName := rowCKCol["CONSTRAINT_NAME"]

			// 匹配替换
			for _, rowCol := range r.TableColumnINFO {
				replaceRex, err := regexp.Compile(fmt.Sprintf("(?i)%v", rowCol["COLUMN_NAME"]))
				if err != nil {
					return nil, err
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

			// 排除非空约束检查
			s := strings.TrimSpace(searchCond)

			if !reg.MatchString(s) {
				if !matchRex.MatchString(s) {
					checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
						constraintName,
						searchCond))
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

				ck := fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
					constraintName,
					strings.Join(d, " "))
				checkKeys = append(checkKeys, ck)
			}
		}
	}

	return checkKeys, nil
}

func (r *Rule) GenTableUniqueIndex() (uniqueIndexes []string, compatibilityIndexSQL []string, err error) {
	if len(r.UniqueIndexINFO) > 0 {
		for _, idxMeta := range r.UniqueIndexINFO {
			var columnList string
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
				columnList = strings.ToLower(idxMeta["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
				columnList = strings.ToUpper(idxMeta["COLUMN_LIST"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
				columnList = idxMeta["COLUMN_LIST"]
			}
			if idxMeta["TABLE_NAME"] != "" && strings.EqualFold(idxMeta["UNIQUENESS"], "UNIQUE") {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					var uniqueIndex []string
					for _, col := range strings.Split(columnList, ",") {
						uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
					}

					uniqueIDX := fmt.Sprintf("UNIQUE INDEX `%s` (%s)", idxMeta["INDEX_NAME"], strings.Join(uniqueIndex, ","))
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
					schema, err := r.GenSchemaName()
					if err != nil {
						return uniqueIndexes, compatibilityIndexSQL, err
					}
					table, err := r.GenTableName()
					if err != nil {
						return uniqueIndexes, compatibilityIndexSQL, err
					}
					sql := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s);",
						idxMeta["INDEX_NAME"], schema, table,
						columnList)

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

				case "NORMAL/REV":
					schema, err := r.GenSchemaName()
					if err != nil {
						return uniqueIndexes, compatibilityIndexSQL, err
					}
					table, err := r.GenTableName()
					if err != nil {
						return uniqueIndexes, compatibilityIndexSQL, err
					}
					sql := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s) REVERSE;",
						idxMeta["INDEX_NAME"], schema, table,
						columnList)

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

			var (
				columnList string
				itypOwner  string
				itypName   string
			)
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
				columnList = strings.ToLower(idxMeta["COLUMN_LIST"])
				itypOwner = strings.ToLower(idxMeta["ITYP_OWNER"])
				itypName = strings.ToLower(idxMeta["ITYP_NAME"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
				columnList = strings.ToUpper(idxMeta["COLUMN_LIST"])
				itypOwner = strings.ToUpper(idxMeta["ITYP_OWNER"])
				itypName = strings.ToUpper(idxMeta["ITYP_NAME"])
			}
			if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameOriginCase) {
				columnList = idxMeta["COLUMN_LIST"]
				itypOwner = idxMeta["ITYP_OWNER"]
				itypName = idxMeta["ITYP_NAME"]
			}

			if idxMeta["TABLE_NAME"] != "" && strings.EqualFold(idxMeta["UNIQUENESS"], "NONUNIQUE") {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					var normalIndex []string
					for _, col := range strings.Split(columnList, ",") {
						normalIndex = append(normalIndex, fmt.Sprintf("`%s`", col))
					}

					keyIndex := fmt.Sprintf("KEY `%s` (%s)", idxMeta["INDEX_NAME"], strings.Join(normalIndex, ","))

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
					schema, err := r.GenSchemaName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					table, err := r.GenTableName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}

					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
						idxMeta["INDEX_NAME"], schema, table,
						columnList)

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
					schema, err := r.GenSchemaName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					table, err := r.GenTableName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						idxMeta["INDEX_NAME"], schema, table,
						columnList)
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
					schema, err := r.GenSchemaName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					table, err := r.GenTableName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						idxMeta["INDEX_NAME"], schema, table,
						columnList)

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
					schema, err := r.GenSchemaName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					table, err := r.GenTableName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');",
						idxMeta["INDEX_NAME"], schema, table,
						columnList,
						itypOwner,
						itypName,
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

				case "NORMAL/REV":
					schema, err := r.GenSchemaName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					table, err := r.GenTableName()
					if err != nil {
						return normalIndexes, compatibilityIndexSQL, err
					}
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) REVERSE;",
						idxMeta["INDEX_NAME"], schema, table,
						columnList)
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
	if len(r.TableCommentINFO) > 0 && r.TableCommentINFO[0]["COMMENTS"] != "" {
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
		columnName := rowCol["COLUMN_NAME"]

		if r.OracleCollation {
			// 字段排序规则检查
			if collationMapVal, ok := common.MigrateTableStructureDatabaseCollationMap[common.TaskTypeOracle2TiDB][strings.ToUpper(rowCol["COLLATION"])][common.MigrateTableStructureDatabaseCharsetMap[common.TaskTypeOracle2TiDB][r.SourceDBCharset]]; ok {
				columnCollation = collationMapVal
			} else {
				// 字段数值数据类型不存在排序规则，排除忽略
				if !strings.EqualFold(rowCol["COLLATION"], "") {
					return tableColumns, fmt.Errorf(`error on check oracle column [%v] collation: %v`, columnName, rowCol["COLLATION"])
				}
				columnCollation = ""
			}
		} else {
			// oracle 12.2 版本以下不支持，置空
			columnCollation = ""
		}

		if val, ok := r.TableColumnDatatypeRule[columnName]; ok {
			columnType = val
		} else {
			return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] data type isn't exist", r.SourceSchemaName, r.SourceTableName, columnName)
		}

		if strings.EqualFold(rowCol["NULLABLE"], "Y") {
			nullable = "NULL"
		} else {
			nullable = "NOT NULL"
		}

		if !strings.EqualFold(rowCol["COMMENTS"], "") {
			r := strings.NewReplacer("'", "\"", "\\", "/")
			comment = "'" + r.Replace(rowCol["COMMENTS"]) + "'"
		} else {
			comment = rowCol["COMMENTS"]
		}

		// 判断表字段值来源
		fromSource, okFromSource := r.TableColumnDefaultValSourceRule[columnName]
		defaultVal, okDefaultVal := r.TableColumnDefaultValRule[columnName]
		if !okFromSource || !okDefaultVal {
			return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] default value isn't exist or default value from source panic", r.SourceSchemaName, r.SourceTableName, columnName)
		}

		if fromSource {
			// 截取数据
			// 字符数据处理 MigrateStringDataTypeDatabaseCharsetMap
			isTrunc := false
			if strings.HasPrefix(defaultVal, "'") && strings.HasSuffix(defaultVal, "'") {
				isTrunc = true
				defaultVal = defaultVal[1 : len(defaultVal)-1]
			}
			convertUtf8Raw, err := common.CharsetConvert([]byte(defaultVal), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(r.SourceDBCharset)], common.CharsetUTF8MB4)
			if err != nil {
				return tableColumns, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
			}

			convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(r.TargetDBCharset)])
			if err != nil {
				return tableColumns, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
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
			// 截取数据
			// 字符数据处理 MigrateStringDataTypeDatabaseCharsetMap
			isTrunc := false
			if strings.HasPrefix(defaultVal, "'") && strings.HasSuffix(defaultVal, "'") {
				isTrunc = true
				defaultVal = defaultVal[1 : len(defaultVal)-1]
			}
			// meta database 数据字符集 utf8mb4
			convertUtf8Raw, err := common.CharsetConvert([]byte(defaultVal), common.CharsetUTF8MB4, common.CharsetUTF8MB4)
			if err != nil {
				return tableColumns, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
			}

			convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(r.TargetDBCharset)])
			if err != nil {
				return tableColumns, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
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

		// 字段名
		if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameLowerCase) {
			columnName = strings.ToLower(columnName)
		}
		if strings.EqualFold(r.LowerCaseFieldName, common.MigrateTableStructFieldNameUpperCase) {
			columnName = strings.ToUpper(columnName)
		}

		if strings.EqualFold(nullable, "NULL") {
			switch {
			case columnCollation != "" && comment != "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s COMMENT %s", columnName, columnType, columnCollation, comment))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s COMMENT %s", columnName, columnType, columnCollation, dataDefault, comment))
				}
			case columnCollation != "" && comment == "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s", columnName, columnType, columnCollation))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s", columnName, columnType, columnCollation, dataDefault))
				}
			case columnCollation == "" && comment != "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COMMENT %s", columnName, columnType, comment))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", columnName, columnType, dataDefault, comment))
				}
			case columnCollation == "" && comment == "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s", columnName, columnType))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault))
				}
			default:
				return tableColumns, fmt.Errorf("error on gen oracle schema table column meta with nullable, rule: %v", r.String())
			}
		} else {
			switch {
			case columnCollation != "" && comment != "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s COMMENT %s", columnName, columnType, columnCollation, nullable, comment))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s COMMENT %s", columnName, columnType, columnCollation, nullable, dataDefault, comment))
				}
			case columnCollation != "" && comment == "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s", columnName, columnType, columnCollation, nullable))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s", columnName, columnType, columnCollation, nullable, dataDefault))
				}
			case columnCollation == "" && comment != "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s COMMENT %s", columnName, columnType, nullable, comment))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", columnName, columnType, nullable, dataDefault, comment))
				}
			case columnCollation == "" && comment == "":
				if strings.EqualFold(dataDefault, common.OracleNULLSTRINGTableAttrWithoutNULL) {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable))
				} else {
					tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault))
				}
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

func (r *Rule) GenSchemaName() (string, error) {
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

	var schema string
	if targetSchema == "" {
		schema = sourceSchema
	}
	if targetSchema != "" {
		schema = targetSchema
	}
	convUtf8Raw, err := common.CharsetConvert([]byte(schema), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(r.SourceDBCharset)], common.CharsetUTF8MB4)
	if err != nil {
		return schema, fmt.Errorf("schema name [%s] charset convert failed, %v", schema, err)
	}

	convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(r.TargetDBCharset)])
	if err != nil {
		return schema, fmt.Errorf("schema name [%s] charset convert failed, %v", schema, err)
	}
	schema = string(convTargetRaw)

	return schema, nil
}

func (r *Rule) GenTableName() (string, error) {
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

	var table string
	if targetTable == "" {
		table = sourceTable
	}
	if targetTable != "" {
		table = targetTable
	}

	convUtf8Raw, err := common.CharsetConvert([]byte(table), common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(r.SourceDBCharset)], common.CharsetUTF8MB4)
	if err != nil {
		return table, fmt.Errorf("table name [%s] charset convert failed, %v", table, err)
	}

	convTargetRaw, err := common.CharsetConvert(convUtf8Raw, common.CharsetUTF8MB4, common.MigrateMYSQLCompatibleCharsetStringConvertMapping[common.StringUPPER(r.TargetDBCharset)])
	if err != nil {
		return table, fmt.Errorf("table name [%s] charset convert failed, %v", table, err)
	}
	table = string(convTargetRaw)

	return table, nil
}

func (r *Rule) String() string {
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr)
}
