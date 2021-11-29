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
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

// 任务
type Table struct {
	SourceSchemaName string
	TargetSchemaName string
	SourceTableName  string
	TargetTableName  string
	SourceTableType  string
	Overwrite        bool
	Engine           *service.Engine `json:"-"`
}

type ColumnTypeMap struct {
	SourceColumnType string
	TargetColumnType string
}

type FileMW struct {
	Mutex  sync.Mutex
	Writer io.Writer
}

func (d *FileMW) Write(b []byte) (n int, err error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	return d.Writer.Write(b)
}

func (t Table) GenerateAndExecMySQLCreateSQL() (string, string, error) {
	tablesMap, err := t.Engine.GetOracleTableComment(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return "", "", err
	}

	service.Logger.Info("get oracle table comment",
		zap.String("schema", t.SourceSchemaName),
		zap.String("table", t.SourceTableName),
		zap.String("comments", fmt.Sprintf("%v", tablesMap)))

	columnMetaSlice, err := t.reverserOracleTableColumnToMySQL()
	if err != nil {
		return "", "", err
	}
	service.Logger.Info("reverse oracle table column",
		zap.String("schema", t.SourceSchemaName),
		zap.String("table", t.SourceTableName),
		zap.Strings("columns", columnMetaSlice))

	primaryKeyMetaSlice, err := t.reverserOracleTablePKToMySQL()
	if err != nil {
		return "", "", err
	}
	service.Logger.Info("reverse oracle table column",
		zap.String("schema", t.SourceSchemaName),
		zap.String("table", t.SourceTableName),
		zap.Strings("pk", primaryKeyMetaSlice))

	var (
		tableMetas     []string
		sqls           strings.Builder // reverse.sql
		builder        strings.Builder // compatibility.sql
		createTableSQL string
	)
	tableMetas = append(tableMetas, columnMetaSlice...)
	tableMetas = append(tableMetas, primaryKeyMetaSlice...)
	tableMeta := strings.Join(tableMetas, ",\n")

	tableComment := tablesMap[0]["COMMENTS"]

	// 创建表初始语句 SQL
	modifyTableName := changeOracleTableName(t.SourceTableName, t.TargetTableName)
	if tableComment != "" {
		createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin  COMMENT='%s'",
			t.TargetSchemaName, modifyTableName, tableMeta, tableComment)
	} else {
		createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			t.TargetSchemaName, modifyTableName, tableMeta)
	}

	service.Logger.Info("reverse",
		zap.String("schema", t.TargetSchemaName),
		zap.String("table", modifyTableName),
		zap.String("create sql", createTableSQL))

	// 表语句 With 主键约束
	sqls.WriteString("/*\n")
	sqls.WriteString(fmt.Sprintf(" oracle table reverse sql \n"))

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"#", "ORACLE TABLE TYPE", "ORACLE", "MYSQL", "SUGGEST"})
	sw.AppendRows([]table.Row{
		{"TABLE", t.SourceTableType, fmt.Sprintf("%s.%s", t.SourceSchemaName, t.SourceTableName), fmt.Sprintf("%s.%s", t.TargetSchemaName, modifyTableName), "Create Table"},
	})
	sqls.WriteString(fmt.Sprintf("%v\n", sw.Render()))
	sqls.WriteString("*/\n")
	sqls.WriteString(createTableSQL + ";\n\n")

	// 唯一约束
	ukMetas, err := t.reverserOracleTableUKToMySQL()
	if err != nil {
		return "", "", err
	}
	if len(ukMetas) > 0 {
		for _, ukMeta := range ukMetas {
			ukSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", t.TargetSchemaName, modifyTableName, ukMeta)
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("create uk", ukSQL))

			sqls.WriteString(ukSQL + ";\n")
		}
	}

	// 唯一索引
	createUniqueIndexSQL, compatibilityUniqueIndexSQL, err := t.reverserOracleTableUniqueIndexToMySQL(
		modifyTableName)
	if err != nil {
		return "", "", err
	}
	if len(createUniqueIndexSQL) > 0 {
		for _, indexSQL := range createUniqueIndexSQL {
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("create unique index", indexSQL))

			sqls.WriteString(indexSQL + ";\n")
		}
	}

	// 普通索引【普通索引、函数索引、位图索引】
	createIndexSQL, compatibilityIndexSQL, err := t.reverserOracleTableNormalIndexToMySQL(modifyTableName)
	if err != nil {
		return "", "", err
	}
	if len(createIndexSQL) > 0 {
		for _, indexSQL := range createIndexSQL {
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("create normal index", indexSQL))

			sqls.WriteString(indexSQL + ";\n")
		}
	}

	// 外键约束、检查约束
	version, err := t.Engine.GetMySQLDBVersion()
	if err != nil {
		return "", "", err
	}
	// 是否 TiDB 版本
	isTiDB := false
	if strings.Contains(version, "TiDB") {
		isTiDB = true
	}

	var dbVersion string

	if strings.Contains(version, utils.MySQLVersionDelimiter) {
		dbVersion = strings.Split(version, utils.MySQLVersionDelimiter)[0]
	} else {
		dbVersion = version
	}

	ckMetas, err := t.reverserOracleTableCKToMySQL()
	if err != nil {
		return "", "", err
	}

	fkMetas, err := t.reverserOracleTableFKToMySQL()
	if err != nil {
		return "", "", err
	}

	if len(fkMetas) > 0 || len(ckMetas) > 0 || len(compatibilityIndexSQL) > 0 || len(compatibilityUniqueIndexSQL) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table consrtaint maybe mysql has compatibility, skip\n"))
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
		tw.AppendRows([]table.Row{
			{"TABLE", fmt.Sprintf("%s.%s", t.SourceSchemaName, t.SourceTableName), fmt.Sprintf("%s.%s", t.TargetSchemaName, modifyTableName), "Create Constraints"}})

		builder.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		builder.WriteString("*/\n")
	}

	if !isTiDB {
		if utils.VersionOrdinal(dbVersion) > utils.VersionOrdinal(utils.MySQLCheckConsVersion) {
			if len(ckMetas) > 0 {
				for _, ck := range ckMetas {
					ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, ck)
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("ck sql", ckSQL))

					sqls.WriteString(ckSQL + "\n")
				}
			}

			if len(fkMetas) > 0 {
				for _, fk := range fkMetas {
					addFkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, fk)
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("fk sql", addFkSQL))

					sqls.WriteString(addFkSQL + "\n")
				}
			}
			// 增加不兼容的索引语句
			if len(compatibilityUniqueIndexSQL) > 0 {
				for _, compSQL := range compatibilityUniqueIndexSQL {
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("maybe compatibility sql", compSQL))

					builder.WriteString(compSQL + ";\n")
				}
			}
			if len(compatibilityIndexSQL) > 0 {
				for _, compSQL := range compatibilityIndexSQL {
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("maybe compatibility sql", compSQL))

					builder.WriteString(compSQL + ";\n")
				}
			}
		} else {
			if len(fkMetas) > 0 {
				for _, fk := range fkMetas {
					addFkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, fk)
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("fk sql", addFkSQL))

					sqls.WriteString(addFkSQL + "\n")
				}
			}
			if len(ckMetas) > 0 {
				builder.WriteString("/*\n")
				builder.WriteString(fmt.Sprintf(" oracle table consrtaint maybe mysql has compatibility, skip\n"))
				tw := table.NewWriter()
				tw.SetStyle(table.StyleLight)
				tw.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
				tw.AppendRows([]table.Row{
					{"TABLE", fmt.Sprintf("%s.%s", t.SourceSchemaName, t.SourceTableName), fmt.Sprintf("%s.%s", t.TargetSchemaName, modifyTableName), "Create Constraints"},
				})
				builder.WriteString(fmt.Sprintf("%v\n", tw.Render()))
				builder.WriteString("*/\n")

				for _, ck := range ckMetas {
					ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, ck)
					builder.WriteString(ckSQL + "\n")
				}
			}
			// 增加不兼容的索引语句
			if len(compatibilityUniqueIndexSQL) > 0 {
				for _, compSQL := range compatibilityUniqueIndexSQL {
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("maybe compatibility sql", compSQL))

					builder.WriteString(compSQL + ";\n")
				}
			}
			if len(compatibilityIndexSQL) > 0 {
				for _, compSQL := range compatibilityIndexSQL {
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("maybe compatibility sql", compSQL))

					builder.WriteString(compSQL + ";\n")
				}
			}
		}
		return sqls.String(), builder.String(), nil
	}

	if len(fkMetas) > 0 {
		for _, fk := range fkMetas {
			fkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, fk)
			builder.WriteString(fkSQL + "\n")
		}
	}

	if len(ckMetas) > 0 {
		for _, ck := range ckMetas {
			ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;", t.TargetSchemaName, modifyTableName, ck)
			builder.WriteString(ckSQL + "\n")
		}
	}
	// 可能不兼容 SQL
	if len(compatibilityUniqueIndexSQL) > 0 {
		for _, compSQL := range compatibilityUniqueIndexSQL {
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("maybe compatibility sql", compSQL))

			builder.WriteString(compSQL + ";\n")
		}
	}
	if len(compatibilityIndexSQL) > 0 {
		for _, compSQL := range compatibilityIndexSQL {
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("maybe compatibility sql", compSQL))
			builder.WriteString(compSQL + ";\n")
		}
	}

	return sqls.String(), builder.String(), nil
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
			if idxMeta["TABLE_NAME"] != "" {
				if idxMeta["UNIQUENESS"] == "NONUNIQUE" {
					switch idxMeta["INDEX_TYPE"] {
					case "NORMAL":
						var normalIndex []string
						for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
							normalIndex = append(normalIndex, fmt.Sprintf("`%s`", col))
						}
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
							strings.ToLower(strings.Join(normalIndex, ","))))

						continue

					case "FUNCTION-BASED NORMAL":
						var normalIndex []string
						for _, col := range strings.Split(idxMeta["COLUMN_EXPRESSION"], ",") {
							normalIndex = append(normalIndex, fmt.Sprintf("`%s`", col))
						}

						compatibilityIndexSQL = append(compatibilityIndexSQL, fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
							strings.ToLower(strings.Join(normalIndex, ","))))

						service.Logger.Warn("reverse normal index",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("index name", idxMeta["INDEX_NAME"]),
							zap.String("index type", idxMeta["INDEX_TYPE"]),
							zap.String("index column list", idxMeta["COLUMN_LIST"]),
							zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
							zap.String("warn", "mysql not support"))
						continue

					case "BITMAP":
						var normalIndex []string
						for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
							normalIndex = append(normalIndex, fmt.Sprintf("`%s`", col))
						}
						compatibilityIndexSQL = append(compatibilityIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
							strings.ToLower(strings.Join(normalIndex, ","))))

						service.Logger.Warn("reverse normal index",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("index name", idxMeta["INDEX_NAME"]),
							zap.String("index type", idxMeta["INDEX_TYPE"]),
							zap.String("index column list", idxMeta["COLUMN_LIST"]),
							zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
							zap.String("warn", "mysql not support"))
						continue

					default:
						service.Logger.Error("reverse normal index",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("index name", idxMeta["INDEX_NAME"]),
							zap.String("index type", idxMeta["INDEX_TYPE"]),
							zap.String("index column list", idxMeta["COLUMN_LIST"]),
							zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
							zap.String("error", "mysql not support"))

						return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("[NORMAL NONUNIQUE] oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
							t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])
					}
				} else {
					service.Logger.Error("reverse normal index",
						zap.String("schema", t.TargetTableName),
						zap.String("table", modifyTableName),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
						zap.String("error", "UNIQUE KEY shouldn't be exist normal index"))

					return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("[NORMAL UNIQUE] oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
						t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])
				}
			}

			service.Logger.Error("reverse normal index",
				zap.String("schema", t.TargetTableName),
				zap.String("table", modifyTableName),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]),
				zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
				zap.Error(fmt.Errorf("[NORMAL] oracle schema [%s] table [%s] can't null", t.SourceSchemaName, t.SourceTableName)))
			return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("[NORMAL] oracle schema [%s] table [%s] can't null",
				t.SourceSchemaName, t.SourceTableName)
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
			if idxMeta["TABLE_NAME"] != "" {
				if idxMeta["UNIQUENESS"] == "NONUNIQUE" {

					service.Logger.Error("reverse unique key",
						zap.String("schema", t.TargetTableName),
						zap.String("table", modifyTableName),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
						zap.String("error", "NORMAL INDEX shouldn't be exist unique key"))

					return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
						t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])

				} else {
					switch idxMeta["INDEX_TYPE"] {
					case "NORMAL":
						var uniqueIndex []string
						for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
							uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
						}

						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]),
							t.TargetSchemaName, modifyTableName,
							strings.ToLower(strings.Join(uniqueIndex, ","))))
						continue

					case "FUNCTION-BASED NORMAL":
						var uniqueIndex []string
						for _, col := range strings.Split(idxMeta["COLUMN_EXPRESSION"], ",") {
							uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
						}

						compatibilityIndexSQL = append(compatibilityIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
							strings.ToLower(strings.Join(uniqueIndex, ","))))

						service.Logger.Warn("reverse unique key",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("index name", idxMeta["INDEX_NAME"]),
							zap.String("index type", idxMeta["INDEX_TYPE"]),
							zap.String("index column list", idxMeta["COLUMN_LIST"]),
							zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
							zap.String("warn", "mysql not support"))
						continue

					case "BITMAP":
						var uniqueIndex []string
						for _, col := range strings.Split(idxMeta["COLUMN_LIST"], ",") {
							uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
						}

						compatibilityIndexSQL = append(compatibilityIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName,
							strings.ToLower(strings.Join(uniqueIndex, ","))))

						service.Logger.Warn("reverse unique key",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("index name", idxMeta["INDEX_NAME"]),
							zap.String("index type", idxMeta["INDEX_TYPE"]),
							zap.String("index column list", idxMeta["COLUMN_LIST"]),
							zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
							zap.String("warn", "mysql not support"))
						continue

					default:
						service.Logger.Error("reverse unique key",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("index name", idxMeta["INDEX_NAME"]),
							zap.String("index type", idxMeta["INDEX_TYPE"]),
							zap.String("index column list", idxMeta["COLUMN_LIST"]),
							zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
							zap.String("error", "mysql not support"))

						return createIndexSQL, compatibilityIndexSQL,
							fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
								t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])
					}
				}
			}
			service.Logger.Error("reverse unique key",
				zap.String("schema", t.TargetTableName),
				zap.String("table", modifyTableName),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]),
				zap.String("index expression", idxMeta["COLUMN_EXPRESSION"]),
				zap.Error(fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] can't null", t.SourceSchemaName, t.SourceTableName)))
			return createIndexSQL, compatibilityIndexSQL,
				fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] can't null", t.SourceSchemaName, t.SourceTableName)
		}
	}

	return createIndexSQL, compatibilityIndexSQL, err
}

func (t Table) reverserOracleTableColumnToMySQL() ([]string, error) {
	var (
		// 字段元数据组
		columnMetas []string
	)

	// 获取表数据字段列信息
	columnsMap, err := t.Engine.GetOracleTableColumn(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return columnMetas, err
	}

	// Oracle 表字段数据类型内置映射 MySQL/TiDB 规则转换
	for _, rowCol := range columnsMap {
		columnMeta, err := ReverseOracleTableColumnMapRule(
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
			t.Engine,
		)
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
		pk := fmt.Sprintf("PRIMARY KEY (%s)", strings.ToLower(strings.Join(pkArr, ",")))
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
				strings.ToLower(rowUKCol["CONSTRAINT_NAME"]), strings.ToLower(strings.Join(ukArr, ",")))

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
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s)",
					strings.ToLower(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToLower(rowFKCol["COLUMN_LIST"]),
					strings.ToLower(rowFKCol["R_OWNER"]),
					strings.ToLower(rowFKCol["RTABLE_NAME"]),
					strings.ToLower(rowFKCol["RCOLUMN_LIST"]))
				keysMeta = append(keysMeta, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE CASCADE",
					strings.ToLower(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToLower(rowFKCol["COLUMN_LIST"]),
					strings.ToLower(rowFKCol["R_OWNER"]),
					strings.ToLower(rowFKCol["RTABLE_NAME"]),
					strings.ToLower(rowFKCol["RCOLUMN_LIST"]))
				keysMeta = append(keysMeta, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE SET NULL",
					strings.ToLower(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToLower(rowFKCol["COLUMN_LIST"]),
					strings.ToLower(rowFKCol["R_OWNER"]),
					strings.ToLower(rowFKCol["RTABLE_NAME"]),
					strings.ToLower(rowFKCol["RCOLUMN_LIST"]))
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
						strings.ToLower(rowCKCol["CONSTRAINT_NAME"]),
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
					strings.ToLower(rowCKCol["CONSTRAINT_NAME"]),
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
func LoadOracleToMySQLTableList(engine *service.Engine, exporterTableSlice []string, sourceSchema, targetSchema string, overwrite bool) ([]Table, []string, []string, []string, error) {
	startTime := time.Now()
	service.Logger.Info("load oracle table list start")

	defer func() {
		endTime := time.Now()
		service.Logger.Info("load oracle table list finished",
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

	var (
		wg     sync.WaitGroup
		tables []Table
	)
	wg.Add(len(exporterTableSlice))

	jobsChan := make(chan Table, len(exporterTableSlice))

	for _, ts := range exporterTableSlice {
		go func(tbl, sourceSchemaName, targetSchemaName string, e *service.Engine, ow bool, tableMap map[string]string) {
			// 库名、表名规则
			jobsChan <- Table{
				SourceSchemaName: sourceSchemaName,
				TargetSchemaName: targetSchemaName,
				SourceTableName:  tbl,
				TargetTableName:  tbl,
				SourceTableType:  tableMap[tbl],
				Overwrite:        ow,
				Engine:           e,
			}
			wg.Done()
		}(ts, sourceSchema, targetSchema, engine, overwrite, tablesMap)
	}
	go func() {
		wg.Wait()
		close(jobsChan)
	}()

	for data := range jobsChan {
		tables = append(tables, data)
	}

	return tables, partitionTables, temporaryTables, clusteredTables, nil
}
