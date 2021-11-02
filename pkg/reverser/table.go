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
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

// 任务
type Table struct {
	SourceSchemaName string
	TargetSchemaName string
	Overwrite        bool
	ColumnTypes      []ColumnType
	Engine           *service.Engine
	TableName
}

type TableName struct {
	SourceTableName string
	TargetTableName string
}

type ColumnName struct {
	SourceColumnName string
	TargetColumnName string
}

type ColumnType struct {
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
	if len(tablesMap) > 1 {
		return "", "", fmt.Errorf("oracle schema [%s] table [%s] comments exist multiple values: [%v]", t.SourceSchemaName, t.SourceTableName, tablesMap)
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
		sqls           strings.Builder
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

	// 表语句
	sqls.WriteString("/*\n")
	sqls.WriteString(fmt.Sprintf(" oracle table reverse sql \n"))

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
	sw.AppendRows([]table.Row{
		{"TABLE", fmt.Sprintf("%s.%s", t.SourceTableName, t.SourceTableName), fmt.Sprintf("%s.%s", t.TargetSchemaName, modifyTableName), "Manual"},
	})
	sqls.WriteString(fmt.Sprintf("%v\n", sw.Render()))
	sqls.WriteString("*/\n")
	sqls.WriteString(createTableSQL + ";\n\n")

	// 索引语句、普通索引、函数索引、位图索引
	createIndexSQL, compatibilityIndexSQL, err := t.reverserOracleTableNormalIndexToMySQL(modifyTableName)

	fmt.Printf("test: %v\n", compatibilityIndexSQL)

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

	// 唯一索引
	createUniqueIndexSQL, uniqueIndexColumnsMap, err := t.reverserOracleTableUniqueIndexToMySQL(modifyTableName)
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

	// 判断唯一约束 vs 唯一索引
	ukMetas, err := t.reverserOracleTableUKToMySQL(uniqueIndexColumnsMap)
	if err != nil {
		return "", "", err
	}
	if len(ukMetas) > 0 {
		for _, ukMeta := range ukMetas {
			ukSQL := fmt.Sprintf("ALTER TABLE %s.%s %s", t.TargetSchemaName, modifyTableName, ukMeta)
			service.Logger.Info("reverse",
				zap.String("schema", t.TargetSchemaName),
				zap.String("table", modifyTableName),
				zap.String("create uk", ukSQL))

			sqls.WriteString(ukSQL + ";\n")
		}
	}

	// 判断外键以及检查约束
	version, err := t.Engine.GetMySQLDBVersion()
	if err != nil {
		return "", "", err
	}
	// 是否 TiDB 版本
	isTiDB := false
	if strings.Contains(version, "TiDB") {
		isTiDB = true
	}

	var (
		builder   strings.Builder
		dbVersion string
	)
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

	if !isTiDB {
		if utils.VersionOrdinal(dbVersion) > utils.VersionOrdinal(utils.MySQLCheckConsVersion) && len(ckMetas) > 0 {
			for _, ck := range ckMetas {
				ckSQL := fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", t.TargetSchemaName, modifyTableName, ck)
				service.Logger.Info("reverse",
					zap.String("schema", t.TargetSchemaName),
					zap.String("table", modifyTableName),
					zap.String("ck sql", ckSQL))

				sqls.WriteString(ckSQL + "\n")
			}
			if len(fkMetas) > 0 {
				for _, fk := range fkMetas {
					addFkSQL := fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", t.TargetSchemaName, modifyTableName, fk)
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("fk sql", addFkSQL))

					sqls.WriteString(addFkSQL + "\n")
				}
			}
			// 增加不兼容的索引语句
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
					addFkSQL := fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", t.TargetSchemaName, modifyTableName, fk)
					service.Logger.Info("reverse",
						zap.String("schema", t.TargetSchemaName),
						zap.String("table", modifyTableName),
						zap.String("fk sql", addFkSQL))

					sqls.WriteString(addFkSQL + "\n")
				}
			}
			if len(ckMetas) > 0 {
				builder.WriteString("/*\n")
				builder.WriteString(fmt.Sprintf(" oracle table check consrtaint maybe mysql has compatibility, skip\n"))
				tw := table.NewWriter()
				tw.SetStyle(table.StyleLight)
				tw.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
				tw.AppendRows([]table.Row{
					{"TABLE", fmt.Sprintf("%s.%s", t.SourceTableName, t.SourceTableName), fmt.Sprintf("%s.%s", t.TargetSchemaName, modifyTableName), "Manual Create"},
				})
				builder.WriteString(fmt.Sprintf("%v\n", tw.Render()))
				builder.WriteString("*/\n")

				for _, ck := range ckMetas {
					ckSQL := fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", t.TargetSchemaName, modifyTableName, ck)
					builder.WriteString(ckSQL + "\n")
				}
			}
			// 增加不兼容的索引语句
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
		// 记录不为空
		if builder.String() != "" {
			builder.WriteString(fmt.Sprintf("\n-- the above info comes from oracle table [%s.%s]\n", t.SourceSchemaName, t.SourceTableName))
			builder.WriteString(fmt.Sprintf("-- the above info comes from mysql table [%s.%s]\n", t.TargetSchemaName, modifyTableName))
		}

		if sqls.String() != "" {
			sqls.WriteString(fmt.Sprintf("\n-- the above info create mysql table sql [%s.%s]\n", t.TargetSchemaName, modifyTableName))
		}
		return sqls.String(), builder.String(), nil
	}

	if len(fkMetas) > 0 || len(ckMetas) > 0 {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table check consrtaint maybe mysql has compatibility, skip\n"))
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
		tw.AppendRows([]table.Row{
			{"TABLE", fmt.Sprintf("%s.%s", t.SourceTableName, t.SourceTableName), fmt.Sprintf("%s.%s", t.TargetSchemaName, modifyTableName), "Manual Create"}})
		builder.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		builder.WriteString("*/\n")

		if len(fkMetas) > 0 {
			for _, fk := range fkMetas {
				fkSQL := fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", t.TargetSchemaName, modifyTableName, fk)
				builder.WriteString(fkSQL + "\n")
			}
		}

		if len(ckMetas) > 0 {
			for _, ck := range ckMetas {
				ckSQL := fmt.Sprintf("ALTER TABLE %s.%s ADD %s;", t.TargetSchemaName, modifyTableName, ck)
				builder.WriteString(ckSQL + "\n")
			}
		}
		// 可能不兼容 SQL
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

	// 记录不为空
	if builder.String() != "" {
		builder.WriteString(fmt.Sprintf("\n-- the above info comes from oracle table [%s.%s]\n", t.SourceSchemaName, t.SourceTableName))
		builder.WriteString(fmt.Sprintf("-- the above info comes from mysql table [%s.%s]\n", t.TargetSchemaName, modifyTableName))
	}

	if sqls.String() != "" {
		sqls.WriteString(fmt.Sprintf("\n-- the above info create mysql table sql [%s.%s]\n", t.TargetSchemaName, modifyTableName))
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
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName, idxMeta["COLUMN_LIST"]))

						return createIndexSQL, compatibilityIndexSQL, err

					case "FUNCTION-BASED NORMAL":
						compatibilityIndexSQL = append(compatibilityIndexSQL, fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName, idxMeta["COLUMN_EXPRESSION"]))

						service.Logger.Warn("reverse",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("indexName", idxMeta["INDEX_NAME"]),
							zap.String("indexType", "FUNCTION-BASED NORMAL"),
							zap.String("indexExpression", idxMeta["COLUMN_EXPRESSION"]),
							zap.String("error", "MySQL Not Support"))

						return createIndexSQL, compatibilityIndexSQL, err

					case "BITMAP":
						compatibilityIndexSQL = append(compatibilityIndexSQL, fmt.Sprintf("CREATE BITMAP INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName, idxMeta["COLUMN_LIST"]))

						service.Logger.Warn("reverse",
							zap.String("schema", t.TargetTableName),
							zap.String("table", modifyTableName),
							zap.String("indexName", idxMeta["INDEX_NAME"]),
							zap.String("indexType", "BITMAP"),
							zap.String("indexColumn", idxMeta["COLUMN_LIST"]),
							zap.String("error", "MySQL Not Support"))

						return createIndexSQL, compatibilityIndexSQL, err

					default:
						return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
							t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])
					}
				}
				return createIndexSQL, compatibilityIndexSQL, fmt.Errorf("oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
					t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])
			}
		}
	}

	return createIndexSQL, compatibilityIndexSQL, err
}

func (t Table) reverserOracleTableUniqueIndexToMySQL(modifyTableName string) ([]string, map[string]string, error) {
	var (
		createIndexSQL []string
	)
	keysMap := make(map[string]string)

	indexesMap, err := t.Engine.GetOracleTableUniqueIndex(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return createIndexSQL, keysMap, err
	}

	if len(indexesMap) > 0 {
		for _, idxMeta := range indexesMap {
			if idxMeta["TABLE_NAME"] != "" {
				if idxMeta["UNIQUENESS"] == "NONUNIQUE" {
					return createIndexSQL, keysMap, fmt.Errorf("oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
						t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])

				} else {
					if idxMeta["INDEX_TYPE"] == "NORMAL" {
						createIndexSQL = append(createIndexSQL, fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s`(%s)",
							strings.ToUpper(idxMeta["INDEX_NAME"]), t.TargetSchemaName, modifyTableName, strings.ToUpper(idxMeta["COLUMN_LIST"])))

						keysMap[strings.ToUpper(idxMeta["INDEX_NAME"])] = strings.ToUpper(idxMeta["COLUMN_LIST"])

						return createIndexSQL, keysMap, err
					}
					return createIndexSQL, keysMap, fmt.Errorf("oracle schema [%s] table [%s] index [%s] isn't mysql support index type [%v]",
						t.SourceSchemaName, t.SourceTableName, idxMeta["INDEX_NAME"], idxMeta["INDEX_TYPE"])
				}

			}
		}
	}

	return createIndexSQL, keysMap, err
}

func (t Table) reverserOracleTableColumnToMySQL() ([]string, error) {
	var (
		// 字段元数据组
		columnMetas []string
	)

	columnsMap, err := t.Engine.GetOracleTableColumn(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return columnMetas, err
	}

	// oracle build-in data type(20c), backward compatible
	// https://docs.oracle.com/en/database/oracle/oracle-database/20/sqlrf/Data-Types.html#GUID-A3C0D836-BADB-44E5-A5D4-265BA5968483
	// oracle convert mysql data type map https://www.convert-in.com/docs/ora2sql/types-mapping.htm
	// https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABHHAJC
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
			t.ColumnTypes,
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
		pk := fmt.Sprintf("PRIMARY KEY (%s)", strings.ToLower(primaryKeyMap[0]["COLUMN_LIST"]))
		keysMeta = append(keysMeta, pk)
	}
	return keysMeta, nil
}

func (t Table) reverserOracleTableUKToMySQL(uniqueKeysMap map[string]string) ([]string, error) {
	var keysMeta []string
	uniqueKeyMap, err := t.Engine.GetOracleTableUniqueKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}
	if len(uniqueKeyMap) > 0 {
		for _, rowUKCol := range uniqueKeyMap {
			// 排除 oracle 唯一约束以已存在的唯一索引使用
			if _, ok := uniqueKeysMap[strings.ToUpper(rowUKCol["INDEX_NAME"])]; ok {
				if strings.ToUpper(rowUKCol["COLUMN_LIST"]) == uniqueKeysMap[strings.ToUpper(rowUKCol["INDEX_NAME"])] {
					continue
				} else {
					uk := fmt.Sprintf("ADD UNIQUE `%s` (%s)", strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(rowUKCol["COLUMN_LIST"]))
					keysMeta = append(keysMeta, uk)
				}
			} else {
				uk := fmt.Sprintf("ADD UNIQUE `%s` (%s)", strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(rowUKCol["COLUMN_LIST"]))
				keysMeta = append(keysMeta, uk)
			}
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
			ck := fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
				strings.ToLower(rowCKCol["CONSTRAINT_NAME"]),
				strings.ToLower(rowCKCol["SEARCH_CONDITION"]))
			keysMeta = append(keysMeta, ck)
		}
	}

	return keysMeta, nil
}

func ReverseOracleTableColumnMapRule(
	sourceSchema, sourceTableName, columnName, dataType, dataNullable, comments, dataDefault string,
	dataScaleValue, dataPrecisionValue, dataLengthValue string, columnTypes []ColumnType) (string, error) {
	var (
		// 字段元数据
		columnMeta string
		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
		// 转换字段类型
		modifyColumnType string
	)
	dataLength, err := strconv.Atoi(dataLengthValue)
	if err != nil {
		return columnMeta, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_length string to int failed: %v", sourceSchema, sourceTableName, err)
	}
	dataPrecision, err := strconv.Atoi(dataPrecisionValue)
	if err != nil {
		return columnMeta, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_precision string to int failed: %v", sourceSchema, sourceTableName, err)
	}
	dataScale, err := strconv.Atoi(dataScaleValue)
	if err != nil {
		return columnMeta, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_scale string to int failed: %v", sourceSchema, sourceTableName, err)
	}

	switch strings.ToUpper(dataType) {
	case "NUMBER":
		switch {
		case dataScale > 0:
			originColumnType = fmt.Sprintf("NUMBER(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		case dataScale == 0:
			switch {
			case dataPrecision == 0 && dataScale == 0:
				originColumnType = "NUMBER"
				//MySQL column type  NUMERIC would convert to DECIMAL(11,0)
				//buildInColumnType = "NUMERIC"
				buildInColumnType = "DECIMAL(11,0)"
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
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,4)", dataPrecision)
			}
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BFILE":
		originColumnType = "BFILE"
		buildInColumnType = "VARCHAR(255)"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CHAR":
		originColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CHARACTER":
		originColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("CHARACTER(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "CLOB":
		originColumnType = "CLOB"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BLOB":
		originColumnType = "BLOB"
		buildInColumnType = "BLOB"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DATE":
		originColumnType = "DATE"
		buildInColumnType = "DATETIME"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DECIMAL":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DEC":
		switch {
		case dataScale == 0 && dataPrecision == 0:
			originColumnType = "DECIMAL"
			buildInColumnType = "DECIMAL"
		default:
			originColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
			buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "DOUBLE PRECISION":
		originColumnType = "DOUBLE PRECISION"
		buildInColumnType = "DOUBLE PRECISION"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "FLOAT":
		originColumnType = "FLOAT"
		if dataPrecision == 0 {
			buildInColumnType = "FLOAT"
		} else {
			buildInColumnType = "DOUBLE"
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "INTEGER":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "INT":
		originColumnType = "INTEGER"
		buildInColumnType = "INT"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "LONG":
		originColumnType = "LONG"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "LONG RAW":
		originColumnType = "LONG RAW"
		buildInColumnType = "LONGBLOB"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BINARY_FLOAT":
		originColumnType = "BINARY_FLOAT"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "BINARY_DOUBLE":
		originColumnType = "BINARY_DOUBLE"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCHAR":
		originColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("NCHAR(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCHAR VARYING":
		originColumnType = "NCHAR VARYING"
		buildInColumnType = fmt.Sprintf("NCHAR VARYING(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NCLOB":
		originColumnType = "NCLOB"
		buildInColumnType = "TEXT"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NUMERIC":
		originColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		buildInColumnType = fmt.Sprintf("NUMERIC(%d,%d)", dataPrecision, dataScale)
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "NVARCHAR2":
		originColumnType = fmt.Sprintf("NVARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("NVARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "RAW":
		originColumnType = fmt.Sprintf("RAW(%d)", dataLength)
		if dataLength < 256 {
			buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		} else {
			buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "REAL":
		originColumnType = "real"
		buildInColumnType = "DOUBLE"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "ROWID":
		originColumnType = "ROWID"
		buildInColumnType = "CHAR(10)"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "SMALLINT":
		originColumnType = "SMALLINT"
		buildInColumnType = "DECIMAL(38)"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "UROWID":
		originColumnType = "UROWID"
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "VARCHAR2":
		originColumnType = fmt.Sprintf("VARCHAR2(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "VARCHAR":
		originColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		buildInColumnType = fmt.Sprintf("VARCHAR(%d)", dataLength)
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	case "XMLTYPE":
		originColumnType = "XMLTYPE"
		buildInColumnType = "LONGTEXT"
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	default:
		if strings.Contains(dataType, "INTERVAL") {
			originColumnType = dataType
			buildInColumnType = "VARCHAR(30)"
		} else if strings.Contains(dataType, "TIMESTAMP") {
			originColumnType = dataType
			if dataScale == 0 {
				buildInColumnType = "TIMESTAMP"
			} else {
				buildInColumnType = fmt.Sprintf("TIMESTAMP(%d)", dataScale)
			}
		} else {
			originColumnType = dataType
			buildInColumnType = "TEXT"
		}
		modifyColumnType = changeOracleTableColumnType(originColumnType, columnTypes, buildInColumnType)
		columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, dataNullable, comments, dataDefault)
	}
	return columnMeta, nil
}

func changeOracleTableName(sourceTableName string, targetTableName string) string {
	if targetTableName == "" {
		return sourceTableName
	}
	if targetTableName != "" {
		return targetTableName
	}
	return sourceTableName
}

func changeOracleTableColumnType(originColumnType string, columnTypes []ColumnType, buildInColumnType string) string {
	if len(columnTypes) == 0 {
		return buildInColumnType
	}
	for _, ct := range columnTypes {
		if strings.ToUpper(ct.SourceColumnType) == strings.ToUpper(originColumnType) {
			if ct.TargetColumnType == "" {
				return buildInColumnType
			}
			return ct.TargetColumnType
		}
	}
	return buildInColumnType
}

func generateOracleTableColumnMetaByType(columnName, columnType, dataNullable, comments, dataDefault string) string {
	var (
		nullable string
		colMeta  string
	)

	columnName = strings.ToLower(columnName)
	columnType = strings.ToLower(columnType)

	if dataNullable == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

	if nullable == "NULL" {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s COMMENT '%s'", columnName, columnType, dataDefault, comments)
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s COMMENT '%s'", columnName, columnType, comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault)
		case comments == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s", columnName, columnType)
		}
	} else {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT '%s'", columnName, columnType, nullable, dataDefault, comments)
			return colMeta
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s %s COMMENT '%s'", columnName, columnType, nullable, comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault)
			return colMeta
		case comments == "" && dataDefault == "":
			colMeta = fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable)
		}
	}
	return colMeta
}
