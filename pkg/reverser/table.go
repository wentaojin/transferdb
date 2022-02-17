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
package reverser

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

// 任务
type Table struct {
	SourceSchemaName      string
	TargetSchemaName      string
	SourceTableName       string
	TargetTableName       string
	OracleCollation       bool
	SourceSchemaCollation string // 可为空
	SourceTableCollation  string // 可为空
	SourceDBNLSSort       string
	SourceDBNLSComp       string
	SourceTableType       string
	Overwrite             bool
	Engine                *service.Engine `json:"-"`
}

func (t Table) GenCreateTableSQL(modifyTableName string) (string, error) {
	// 表语句 With 主键约束
	tablesMap, err := t.Engine.GetOracleTableComment(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return "", err
	}

	columnMetaSlice, err := t.reverserOracleTableColumnToMySQL(t.OracleCollation)
	if err != nil {
		return "", err
	}

	primaryKeyMetaSlice, err := t.reverserOracleTablePKToMySQL()
	if err != nil {
		return "", err
	}

	var (
		tableMetas     []string
		createTableSQL string
		tableCollation string
	)
	tableMetas = append(tableMetas, columnMetaSlice...)
	tableMetas = append(tableMetas, primaryKeyMetaSlice...)
	tableMeta := strings.Join(tableMetas, ",\n")

	// schema、db、table collation
	if t.OracleCollation {
		// table collation
		if t.SourceTableCollation != "" {
			if val, ok := utils.OracleCollationMap[t.SourceTableCollation]; ok {
				tableCollation = val
			} else {
				return "", fmt.Errorf("oracle table collation [%v] isn't support", t.SourceTableCollation)
			}
		}
		// schema collation
		if t.SourceTableCollation == "" && t.SourceSchemaCollation != "" {
			if val, ok := utils.OracleCollationMap[t.SourceSchemaCollation]; ok {
				tableCollation = val
			} else {
				return "", fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support", t.SourceSchemaCollation, t.SourceTableCollation)
			}
		}
		if t.SourceTableName == "" && t.SourceSchemaCollation == "" {
			return "", fmt.Errorf("oracle schema collation [%v] table collation [%v] isn't support", t.SourceSchemaCollation, t.SourceTableCollation)
		}
	} else {
		// db collation
		if val, ok := utils.OracleCollationMap[t.SourceDBNLSComp]; ok {
			tableCollation = val
		} else {
			return "", fmt.Errorf("oracle db nls_comp [%v] nls_sort [%v] isn't support", t.SourceDBNLSComp, t.SourceDBNLSSort)
		}
	}

	// table 注释
	tableComment := tablesMap[0]["COMMENTS"]

	if tableComment != "" {
		createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s COMMENT='%s';",
			t.TargetSchemaName, modifyTableName, tableMeta, strings.ToLower(utils.MySQLCharacterSet), tableCollation, tableComment)
	} else {
		createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s;",
			t.TargetSchemaName, modifyTableName, tableMeta, strings.ToLower(utils.MySQLCharacterSet), tableCollation)
	}

	service.Logger.Info("reverse oracle table struct",
		zap.String("schema", t.SourceSchemaName),
		zap.String("table", t.SourceTableName),
		zap.String("comment", tablesMap[0]["COMMENTS"]),
		zap.Strings("columns", columnMetaSlice),
		zap.Strings("pk", primaryKeyMetaSlice),
		zap.String("create sql", createTableSQL))

	return createTableSQL, nil
}

func (t Table) GenCreateUKSQL(modifyTableName string) ([]string, error) {
	var ukArr []string
	ukMetas, err := t.reverserOracleTableUKToMySQL()
	if err != nil {
		return ukArr, err
	}
	if len(ukMetas) > 0 {
		for _, ukMeta := range ukMetas {
			ukSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;", t.TargetSchemaName, modifyTableName, ukMeta)
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("create uk", ukSQL))

			ukArr = append(ukArr, ukSQL)
		}
	}
	return ukArr, nil
}

func (t Table) GenCreateFKSQL(modifyTableName string) ([]string, error) {
	var fkArr []string
	fkMetas, err := t.reverserOracleTableFKToMySQL()
	if err != nil {
		return fkArr, err
	}

	if len(fkMetas) > 0 {
		for _, fk := range fkMetas {
			addFkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, fk)
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("fk sql", addFkSQL))

			fkArr = append(fkArr, addFkSQL)
		}
	}
	return fkArr, nil
}

func (t Table) GenCreateCKSQL(modifyTableName string) ([]string, error) {
	var ckArr []string
	ckMetas, err := t.reverserOracleTableCKToMySQL()
	if err != nil {
		return ckArr, err
	}

	if len(ckMetas) > 0 {
		for _, ck := range ckMetas {
			ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, ck)
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("ck sql", ckSQL))

			ckArr = append(ckArr, ckSQL)
		}
	}

	return ckArr, nil
}

func (t Table) GenCreateNonUniqueIndex(modifyTableName string) ([]string, []string, error) {
	// 普通索引【普通索引、函数索引、位图索引】
	return t.reverserOracleTableNormalIndexToMySQL(modifyTableName)
}

func (t Table) GenCreateUniqueIndex(modifyTableName string) ([]string, []string, error) {
	// 唯一索引
	return t.reverserOracleTableUniqueIndexToMySQL(modifyTableName)
}

func (t Table) reverserOracleTableNormalIndexToMySQL(modifyTableName string) ([]string, []string, error) {
	var (
		createIndexSQL        []string
		compatibilityIndexSQL []string
	)
	indexesMap, err := t.Engine.GetOracleTableNormalIndex(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return createIndexSQL, compatibilityIndexSQL, err
	}
	if len(indexesMap) > 0 {
		for _, idxMeta := range indexesMap {
			if idxMeta["TABLE_NAME"] != "" && strings.ToUpper(idxMeta["UNIQUENESS"]) == "NONUNIQUE" {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					var normalIndex []string
					for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
						normalIndex = append(normalIndex, fmt.Sprintf("`%s`", col))
					}

					sql := fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s` (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
						strings.Join(normalIndex, ","))

					createIndexSQL = append(createIndexSQL, sql)

					service.Logger.Info("reverse normal index",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql))

					continue

				case "FUNCTION-BASED NORMAL":
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					service.Logger.Warn("reverse normal index",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "BITMAP":
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					service.Logger.Warn("reverse normal index",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "FUNCTION-BASED BITMAP":
					sql := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					service.Logger.Warn("reverse normal index",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create normal index sql", sql),
						zap.String("warn", "mysql not support"))
					continue

				case "DOMAIN":
					sql := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');",
						strings.ToUpper(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
						idxMeta["COLUMN_LIST"],
						strings.ToUpper(idxMeta["ITYP_OWNER"]),
						strings.ToUpper(idxMeta["ITYP_NAME"]),
						idxMeta["PARAMETERS"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					service.Logger.Warn("reverse normal index",
						zap.String("schema", t.SourceSchemaName),
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
					service.Logger.Error("reverse normal index",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("domain owner", idxMeta["ITYP_OWNER"]),
						zap.String("domain index name", idxMeta["ITYP_NAME"]),
						zap.String("domain parameters", idxMeta["PARAMETERS"]),
						zap.String("error", "mysql not support"))

					return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("[NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", t.SourceSchemaName, t.SourceTableName, idxMeta)
				}
			}

			service.Logger.Error("reverse normal index",
				zap.String("schema", t.SourceSchemaName),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]),
				zap.String("domain owner", idxMeta["ITYP_OWNER"]),
				zap.String("domain index name", idxMeta["ITYP_NAME"]),
				zap.String("domain parameters", idxMeta["PARAMETERS"]))
			return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("[NON-NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", t.SourceSchemaName, t.SourceTableName, idxMeta)
		}
	}

	return createIndexSQL, compatibilityIndexSQL, err
}

func (t Table) reverserOracleTableUniqueIndexToMySQL(modifyTableName string) ([]string, []string, error) {
	var (
		createIndexSQL        []string
		compatibilityIndexSQL []string
	)

	indexesMap, err := t.Engine.GetOracleTableUniqueIndex(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return createIndexSQL, compatibilityIndexSQL, err
	}

	if len(indexesMap) > 0 {
		for _, idxMeta := range indexesMap {
			if idxMeta["TABLE_NAME"] != "" && strings.ToUpper(idxMeta["UNIQUENESS"]) == "UNIQUE" {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					var uniqueIndex []string
					for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
						uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
					}

					sql := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]),
						t.TargetSchemaName, modifyTableName,
						strings.Join(uniqueIndex, ","))

					createIndexSQL = append(createIndexSQL, sql)

					service.Logger.Info("reverse unique index",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create unique index sql", sql))

					continue

				case "FUNCTION-BASED NORMAL":
					sql := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s);",
						strings.ToUpper(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
						idxMeta["COLUMN_LIST"])

					compatibilityIndexSQL = append(compatibilityIndexSQL, sql)

					service.Logger.Warn("reverse unique key",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create unique index sql", sql),
						zap.String("warn", "mysql not support"))

					continue

				default:
					service.Logger.Error("reverse unique index",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("error", "mysql not support"))

					return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] reverse normal index panic, error: %v", t.SourceSchemaName, t.SourceTableName, idxMeta)
				}
			}
			service.Logger.Error("reverse unique key",
				zap.String("schema", t.SourceSchemaName),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]))
			return createIndexSQL, compatibilityIndexSQL,
				fmt.Errorf("[NON-UNIQUE] oracle schema [%s] table [%s] panic, error: %v", t.SourceSchemaName, t.SourceTableName, idxMeta)
		}
	}

	return createIndexSQL, compatibilityIndexSQL, err
}

func (t Table) reverserOracleTableColumnToMySQL(oraCollation bool) ([]string, error) {
	var (
		// 字段元数据组
		columnMetas []string
	)

	// 获取表数据字段列信息
	columnsMap, err := t.Engine.GetOracleTableColumn(t.SourceSchemaName, t.SourceTableName, oraCollation)
	if err != nil {
		return columnMetas, err
	}

	// Oracle 表字段数据类型内置映射 MySQL/TiDB 规则转换
	for _, rowCol := range columnsMap {
		var (
			columnMeta string
			err        error
		)
		if oraCollation {
			columnMeta, err = ReverseOracleTableColumnMapRule(
				t.SourceSchemaName,
				t.SourceTableName,
				rowCol["COLUMN_NAME"],
				rowCol["DATA_TYPE"],
				rowCol["NULLABLE"],
				rowCol["COMMENTS"],
				rowCol["DATA_DEFAULT"],
				rowCol["DATA_SCALE"],
				rowCol["DATA_PRECISION"],
				rowCol["DATA_LENGTH"],
				rowCol["COLLATION"],
				t.Engine,
			)
		} else {
			columnMeta, err = ReverseOracleTableColumnMapRule(
				t.SourceSchemaName,
				t.SourceTableName,
				rowCol["COLUMN_NAME"],
				rowCol["DATA_TYPE"],
				rowCol["NULLABLE"],
				rowCol["COMMENTS"],
				rowCol["DATA_DEFAULT"],
				rowCol["DATA_SCALE"],
				rowCol["DATA_PRECISION"],
				rowCol["DATA_LENGTH"],
				"",
				t.Engine,
			)
		}
		if err != nil {
			return columnMetas, err
		}
		columnMetas = append(columnMetas, columnMeta)
	}
	return columnMetas, nil
}

func (t Table) reverserOracleTablePKToMySQL() ([]string, error) {
	var keysMeta []string
	primaryKeyMap, err := t.Engine.GetOracleTablePrimaryKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}

	if len(primaryKeyMap) > 1 {
		return keysMeta, fmt.Errorf("oracle schema [%s] table [%s] primary key exist multiple values: [%v]", t.SourceSchemaName, t.SourceTableName, primaryKeyMap)
	}
	if len(primaryKeyMap) > 0 {
		var pkArr []string
		for _, col := range strings.Split(primaryKeyMap[0]["COLUMN_LIST"], ",") {
			pkArr = append(pkArr, fmt.Sprintf("`%s`", col))
		}
		pk := fmt.Sprintf("PRIMARY KEY (%s)", strings.ToUpper(strings.Join(pkArr, ",")))
		keysMeta = append(keysMeta, pk)
	}
	return keysMeta, nil
}

func (t Table) reverserOracleTableUKToMySQL() ([]string, error) {
	var keysMeta []string
	uniqueKeyMap, err := t.Engine.GetOracleTableUniqueKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}
	if len(uniqueKeyMap) > 0 {
		for _, rowUKCol := range uniqueKeyMap {
			var ukArr []string
			for _, col := range strings.Split(rowUKCol["COLUMN_LIST"], ",") {
				ukArr = append(ukArr, fmt.Sprintf("`%s`", col))
			}
			uk := fmt.Sprintf("ADD UNIQUE `%s` (%s)",
				strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(strings.Join(ukArr, ",")))

			keysMeta = append(keysMeta, uk)
		}
	}
	return keysMeta, nil
}

func (t Table) reverserOracleTableFKToMySQL() ([]string, error) {
	var keysMeta []string
	foreignKeyMap, err := t.Engine.GetOracleTableForeignKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}

	if len(foreignKeyMap) > 0 {
		for _, rowFKCol := range foreignKeyMap {
			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s)",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				keysMeta = append(keysMeta, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s`(%s) ON DELETE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				keysMeta = append(keysMeta, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE SET NULL",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				keysMeta = append(keysMeta, fk)
			}
		}
	}

	return keysMeta, nil
}

func (t Table) reverserOracleTableCKToMySQL() ([]string, error) {
	var keysMeta []string
	checkKeyMap, err := t.Engine.GetOracleTableCheckKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}
	if len(checkKeyMap) > 0 {
		for _, rowCKCol := range checkKeyMap {
			// 多个检查约束匹配
			// 比如："LOC" IS noT nUll and loc in ('a','b','c')
			r, err := regexp.Compile(`\s+(?i:AND)\s+|\s+(?i:OR)\s+`)
			if err != nil {
				return keysMeta, fmt.Errorf("check constraint regexp and/or failed: %v", err)
			}

			// 排除非空约束检查
			s := strings.TrimSpace(rowCKCol["SEARCH_CONDITION"])

			if !r.MatchString(s) {
				matchNull, err := regexp.MatchString(`(^.*)(?i:IS NOT NULL)`, s)
				if err != nil {
					return keysMeta, fmt.Errorf("check constraint remove not null failed: %v", err)
				}
				if !matchNull {
					keysMeta = append(keysMeta, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
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
					matchNull, err := regexp.MatchString(`(.*)(?i:IS NOT NULL)`, v)
					if err != nil {
						fmt.Printf("check constraint remove not null failed: %v", err)
					}

					if !matchNull {
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

				keysMeta = append(keysMeta, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
					strings.ToUpper(rowCKCol["CONSTRAINT_NAME"]),
					strings.Join(d, " ")))
			}
		}
	}

	return keysMeta, nil
}

func (t *Table) String() string {
	jsonStr, _ := json.Marshal(t)
	return string(jsonStr)
}

// 加载表列表
func LoadOracleToMySQLTableList(engine *service.Engine, exporterTableSlice []string, sourceSchema, targetSchema, nlsSort, nlsComp string, overwrite bool) ([]Table, []string, []string, []string, error) {
	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		service.Logger.Info("load oracle table list finished",
			zap.String("schema", sourceSchema),
			zap.Int("table totals", len(exporterTableSlice)),
			zap.String("cost", endTime.Sub(startTime).String()))
	}()

	// 筛选过滤可能不支持的表类型
	partitionTables, err := engine.FilterOraclePartitionTable(sourceSchema, exporterTableSlice)
	if err != nil {
		return []Table{}, partitionTables, []string{}, []string{}, err
	}
	temporaryTables, err := engine.FilterOracleTemporaryTable(sourceSchema, exporterTableSlice)
	if err != nil {
		return []Table{}, []string{}, temporaryTables, []string{}, err
	}
	clusteredTables, err := engine.FilterOracleClusteredTable(sourceSchema, exporterTableSlice)
	if err != nil {
		return []Table{}, []string{}, []string{}, clusteredTables, err
	}

	if len(partitionTables) != 0 {
		service.Logger.Warn("partition tables",
			zap.String("schema", sourceSchema),
			zap.String("partition table list", fmt.Sprintf("%v", partitionTables)),
			zap.String("suggest", "if necessary, please manually convert and process the tables in the above list"))
	}
	if len(temporaryTables) != 0 {
		service.Logger.Warn("temporary tables",
			zap.String("schema", sourceSchema),
			zap.String("temporary table list", fmt.Sprintf("%v", temporaryTables)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}
	if len(clusteredTables) != 0 {
		service.Logger.Warn("clustered tables",
			zap.String("schema", sourceSchema),
			zap.String("clustered table list", fmt.Sprintf("%v", clusteredTables)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}

	tablesMap, err := engine.GetOracleTableType(sourceSchema)
	if err != nil {
		return []Table{}, partitionTables, temporaryTables, clusteredTables, err
	}
	endTime := time.Now()
	service.Logger.Info("get oracle table type finished",
		zap.String("schema", sourceSchema),
		zap.Int("table totals", len(exporterTableSlice)),
		zap.String("cost", endTime.Sub(startTime).String()))

	// oracle 环境信息
	characterSet, err := engine.GetOracleDBCharacterSet()
	if err != nil {
		return []Table{}, partitionTables, temporaryTables, clusteredTables, err
	}
	if _, ok := utils.OracleDBCharacterSetMap[strings.Split(characterSet, ".")[1]]; !ok {
		return []Table{}, partitionTables, temporaryTables, clusteredTables, fmt.Errorf("oracle db character set [%v] isn't support", characterSet)
	}

	// oracle 版本是否可指定表、字段 collation
	// oracle db nls_sort/nls_comp 值需要相等，USING_NLS_COMP 值取 nls_comp
	oraDBVersion, err := engine.GetOracleDBVersion()
	if err != nil {
		return []Table{}, partitionTables, temporaryTables, clusteredTables, err
	}

	oraCollation := false
	if utils.VersionOrdinal(oraDBVersion) >= utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
		oraCollation = true
	}

	var (
		tblCollation    map[string]string
		schemaCollation string
	)

	if oraCollation {
		schemaCollation, err = engine.GetOracleSchemaCollation(sourceSchema)
		if err != nil {
			return []Table{}, partitionTables, temporaryTables, clusteredTables, err
		}
		tblCollation, err = engine.GetOracleTableCollation(sourceSchema)
		if err != nil {
			return []Table{}, partitionTables, temporaryTables, clusteredTables, err
		}
	}

	var tables []Table
	for _, ts := range exporterTableSlice {
		// 库名、表名规则
		tbl := Table{
			SourceSchemaName: strings.ToUpper(sourceSchema),
			TargetSchemaName: strings.ToUpper(targetSchema),
			SourceTableName:  strings.ToUpper(ts),
			TargetTableName:  strings.ToUpper(ts),
			SourceTableType:  tablesMap[ts],
			SourceDBNLSSort:  nlsSort,
			SourceDBNLSComp:  nlsComp,
			Overwrite:        overwrite,
			Engine:           engine,
		}
		tbl.OracleCollation = oraCollation
		if oraCollation {
			tbl.SourceSchemaCollation = schemaCollation
			tbl.SourceTableCollation = tblCollation[strings.ToUpper(ts)]
		}
		tables = append(tables, tbl)
	}

	return tables, partitionTables, temporaryTables, clusteredTables, nil
}
