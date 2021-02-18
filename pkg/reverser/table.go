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
	"regexp"
	"strconv"
	"strings"

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/transferdb/db"
)

// 任务
type Table struct {
	SourceSchemaName string
	TargetSchemaName string
	Overwrite        bool
	ColumnTypes      []ColumnType
	Engine           *db.Engine
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

func (t Table) GenerateAndExecMySQLCreateTableSQL() error {
	tablesMap, err := t.Engine.GetOracleTableComment(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return err
	}
	if len(tablesMap) > 1 {
		return fmt.Errorf("oracle schema [%s] table [%s] comments exist multiple values: [%v]", t.SourceSchemaName, t.SourceTableName, tablesMap)
	}
	columnMetaSlice, err := t.reverserOracleTableColumnToMySQL()
	if err != nil {
		return err
	}
	keyMetaSlice, err := t.reverserOracleTableKeyToMySQL()
	if err != nil {
		return err
	}
	var (
		tableMetas     []string
		createTableSQL string
	)
	tableMetas = append(tableMetas, columnMetaSlice...)
	tableMetas = append(tableMetas, keyMetaSlice...)
	tableMeta := strings.Join(tableMetas, ",")

	tableComment := tablesMap[0]["COMMENTS"]
	// 创建表初始语句 SQL
	modifyTableName := changeOracleTableName(t.SourceTableName, t.TargetTableName)
	if tableComment != "" {
		createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin  COMMENT='%s'",
			t.TargetSchemaName, modifyTableName, tableMeta, tableComment)
	} else {
		createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			t.TargetSchemaName, modifyTableName, tableMeta)
	}

	zlog.Logger.Info("exec sql",
		zap.String("schema", t.TargetSchemaName),
		zap.String("table", modifyTableName),
		zap.String("sql", fmt.Sprintf("%v", createTableSQL)))

	_, _, err = db.Query(t.Engine.MysqlDB, createTableSQL)
	if err != nil {
		return err
	}
	return nil
}

func (t Table) GenerateAndExecMySQLCreateIndexSQL() error {
	var (
		createIndexSQL string
	)
	indexesMap, err := t.Engine.GetOracleTableIndex(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return err
	}
	for _, idxMeta := range indexesMap {
		if idxMeta["TABLE_NAME"] != "" {
			ok := t.Engine.IsExistMysqlIndex(t.TargetSchemaName, t.TargetTableName, strings.ToLower(idxMeta["INDEX_NAME"]))
			if !ok {
				// 索引创建
				if idxMeta["UNIQUENESS"] == "NONUNIQUE" {
					createIndexSQL = fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s`(%s)",
						strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, t.TargetTableName, strings.ToLower(idxMeta["COLUMN_LIST"]))
				} else {
					createIndexSQL = fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s`(%s)",
						strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, t.TargetTableName, strings.ToLower(idxMeta["COLUMN_LIST"]))
				}
				zlog.Logger.Info("Exec SQL",
					zap.String("schema", t.TargetTableName),
					zap.String("table", t.TargetTableName),
					zap.String("sql", fmt.Sprintf("%v", createIndexSQL)))
				_, _, err = db.Query(t.Engine.MysqlDB, createIndexSQL)
				if err != nil {
					return err
				}
			} else {
				// 跳过原索引名，重命名索引添加后缀 _ping 创建
				zlog.Logger.Warn("Appear Warning",
					zap.String("schema", t.TargetSchemaName),
					zap.String("table", t.TargetTableName),
					zap.String("index", strings.ToLower(idxMeta["INDEX_NAME"])),
					zap.String("warn", fmt.Sprintf("table index is exist, skip created and rename index created")))

				if idxMeta["UNIQUENESS"] == "NONUNIQUE" {
					createIndexSQL = fmt.Sprintf("CREATE INDEX `%s_ping` ON `%s`.`%s`(%s)",
						strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, t.TargetTableName, strings.ToLower(idxMeta["COLUMN_LIST"]))
				} else {
					createIndexSQL = fmt.Sprintf("CREATE UNIQUE INDEX `%s_ping` ON `%s`.`%s`(%s)",
						strings.ToLower(idxMeta["INDEX_NAME"]), t.TargetSchemaName, t.TargetTableName, strings.ToLower(idxMeta["COLUMN_LIST"]))
				}
				_, _, err = db.Query(t.Engine.MysqlDB, createIndexSQL)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (t Table) reverserOracleTableColumnToMySQL() ([]string, error) {
	var (
		// 字段元数据组
		columnMetas []string
		// 字段元数据
		columnMeta string
		// 字段名
		columnName string
		// oracle 表原始字段类型
		originColumnType string
		// 内置字段类型转换规则
		buildInColumnType string
		// 转换字段类型
		modifyColumnType string
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
		columnName = rowCol["COLUMN_NAME"]
		lengthValue, err := strconv.Atoi(rowCol["DATA_LENGTH"])
		if err != nil {
			return columnMetas, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_length string to int failed: %v", t.SourceTableName, t.SourceTableName, err)
		}
		precisionValue, err := strconv.Atoi(rowCol["DATA_PRECISION"])
		if err != nil {
			return columnMetas, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_precision string to int failed: %v", t.SourceTableName, t.SourceTableName, err)
		}
		scaleValue, err := strconv.Atoi(rowCol["DATA_SCALE"])
		if err != nil {
			return columnMetas, fmt.Errorf("oracle schema [%s] table [%s] reverser column data_scale string to int failed: %v", t.SourceTableName, t.SourceTableName, err)
		}
		switch rowCol["DATA_TYPE"] {
		case "NUMBER":
			switch {
			case scaleValue > 0:
				originColumnType = fmt.Sprintf("NUMBER(%d,%d)", precisionValue, scaleValue)
				buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", precisionValue, scaleValue)
			case scaleValue == 0:
				switch {
				case precisionValue == 0 && scaleValue == 0:
					originColumnType = "NUMBER"
					//MySQL column type  NUMERIC would convert to DECIMAL(11,0)
					//buildInColumnType = "NUMERIC"
					buildInColumnType = "DECIMAL(11,0)"
				case precisionValue >= 1 && precisionValue < 3:
					originColumnType = fmt.Sprintf("NUMBER(%d)", precisionValue)
					buildInColumnType = "TINYINT"
				case precisionValue >= 3 && precisionValue < 5:
					originColumnType = fmt.Sprintf("NUMBER(%d)", precisionValue)
					buildInColumnType = "SMALLINT"
				case precisionValue >= 5 && precisionValue < 9:
					originColumnType = fmt.Sprintf("NUMBER(%d)", precisionValue)
					buildInColumnType = "INT"
				case precisionValue >= 9 && precisionValue < 19:
					originColumnType = fmt.Sprintf("NUMBER(%d)", precisionValue)
					buildInColumnType = "BIGINT"
				case precisionValue >= 19 && precisionValue <= 38:
					originColumnType = fmt.Sprintf("NUMBER(%d)", precisionValue)
					buildInColumnType = fmt.Sprintf("DECIMAL(%d)", precisionValue)
				default:
					originColumnType = fmt.Sprintf("NUMBER(%d)", precisionValue)
					buildInColumnType = fmt.Sprintf("DECIMAL(%d,4)", precisionValue)
				}
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "BFILE":
			originColumnType = "BFILE"
			buildInColumnType = "VARCHAR(255)"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "CHAR":
			originColumnType = fmt.Sprintf("CHAR(%s)", rowCol["DATA_LENGTH"])
			if lengthValue < 256 {
				buildInColumnType = fmt.Sprintf("CHAR(%s)", rowCol["DATA_LENGTH"])
			}
			buildInColumnType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "CHARACTER":
			originColumnType = fmt.Sprintf("CHARACTER(%s)", rowCol["DATA_LENGTH"])
			if lengthValue < 256 {
				buildInColumnType = fmt.Sprintf("CHARACTER(%s)", rowCol["DATA_LENGTH"])
			}
			buildInColumnType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "CLOB":
			originColumnType = "CLOB"
			buildInColumnType = "LONGTEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "BLOB":
			originColumnType = "BLOB"
			buildInColumnType = "BLOB"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "DATE":
			originColumnType = "DATE"
			buildInColumnType = "DATETIME"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "DECIMAL":
			switch {
			case scaleValue == 0 && precisionValue == 0:
				originColumnType = "DECIMAL"
				buildInColumnType = "DECIMAL"
			default:
				originColumnType = fmt.Sprintf("DECIMAL(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
				buildInColumnType = fmt.Sprintf("DECIMAL(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "DEC":
			switch {
			case scaleValue == 0 && precisionValue == 0:
				originColumnType = "DECIMAL"
				buildInColumnType = "DECIMAL"
			default:
				originColumnType = fmt.Sprintf("DECIMAL(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
				buildInColumnType = fmt.Sprintf("DECIMAL(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "DOUBLE PRECISION":
			originColumnType = "DOUBLE PRECISION"
			buildInColumnType = "DOUBLE PRECISION"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "FLOAT":
			originColumnType = "FLOAT"
			if precisionValue == 0 {
				buildInColumnType = "FLOAT"
			}
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "INTEGER":
			originColumnType = "INTEGER"
			buildInColumnType = "INT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "INT":
			originColumnType = "INTEGER"
			buildInColumnType = "INT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "LONG":
			originColumnType = "LONG"
			buildInColumnType = "LONGTEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "LONG RAW":
			originColumnType = "LONG RAW"
			buildInColumnType = "LONGBLOB"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "BINARY_FLOAT":
			originColumnType = "BINARY_FLOAT"
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "BINARY_DOUBLE":
			originColumnType = "BINARY_DOUBLE"
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "NCHAR":
			originColumnType = fmt.Sprintf("NCHAR(%s)", rowCol["DATA_LENGTH"])
			if lengthValue < 256 {
				buildInColumnType = fmt.Sprintf("NCHAR(%s)", rowCol["DATA_LENGTH"])
			}
			buildInColumnType = fmt.Sprintf("NVARCHAR(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "NCHAR VARYING":
			originColumnType = "NCHAR VARYING"
			buildInColumnType = fmt.Sprintf("NCHAR VARYING(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "NCLOB":
			originColumnType = "NCLOB"
			buildInColumnType = "TEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "NUMERIC":
			originColumnType = fmt.Sprintf("NUMERIC(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
			buildInColumnType = fmt.Sprintf("NUMERIC(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "NVARCHAR2":
			originColumnType = fmt.Sprintf("NVARCHAR2(%s)", rowCol["DATA_LENGTH"])
			buildInColumnType = fmt.Sprintf("NVARCHAR(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "RAW":
			originColumnType = fmt.Sprintf("RAW(%s)", rowCol["DATA_LENGTH"])
			if lengthValue < 256 {
				buildInColumnType = fmt.Sprintf("BINARY(%s)", rowCol["DATA_LENGTH"])
			}
			buildInColumnType = fmt.Sprintf("VARBINARY(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "real":
			originColumnType = "real"
			buildInColumnType = "DOUBLE"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "ROWID":
			originColumnType = "ROWID"
			buildInColumnType = "CHAR(10)"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "SMALLINT":
			originColumnType = "SMALLINT"
			buildInColumnType = "DECIMAL(38)"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "UROWID":
			originColumnType = "UROWID"
			buildInColumnType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "VARCHAR2":
			originColumnType = fmt.Sprintf("VARCHAR2(%s)", rowCol["DATA_LENGTH"])
			buildInColumnType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "VARCHAR":
			originColumnType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			buildInColumnType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		case "XMLTYPE":
			originColumnType = "XMLTYPE"
			buildInColumnType = "LONGTEXT"
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		default:
			if strings.Contains(rowCol["DATA_TYPE"], "INTERVAL") {
				originColumnType = rowCol["DATA_TYPE"]
				buildInColumnType = "VARCHAR(30)"
			} else if strings.Contains(rowCol["DATA_TYPE"], "TIMESTAMP") {
				originColumnType = rowCol["DATA_TYPE"]
				if scaleValue == 0 {
					buildInColumnType = "DATETIME"
				}
				buildInColumnType = fmt.Sprintf("DATETIME(%s)", rowCol["DATA_SCALE"])
			} else {
				originColumnType = rowCol["DATA_TYPE"]
				buildInColumnType = "TEXT"
			}
			modifyColumnType = changeOracleTableColumnType(originColumnType, t.ColumnTypes, buildInColumnType)
			columnMeta = generateOracleTableColumnMetaByType(columnName, modifyColumnType, rowCol)
		}
		columnMetas = append(columnMetas, columnMeta)
	}
	return columnMetas, nil
}

func (t Table) reverserOracleTableKeyToMySQL() ([]string, error) {
	var keysMeta []string
	primaryKeyMap, err := t.Engine.GetOracleTablePrimaryKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}
	uniqueKeyMap, err := t.Engine.GetOracleTableUniqueKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}
	foreignKeyMap, err := t.Engine.GetOracleTableForeignKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}
	checkKeyMap, err := t.Engine.GetOracleTableCheckKey(t.SourceSchemaName, t.SourceTableName)
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
	if len(uniqueKeyMap) > 0 {
		for _, rowUKCol := range uniqueKeyMap {
			uk := fmt.Sprintf("UNIQUE KEY `%s` (%s)", strings.ToLower(rowUKCol["CONSTRAINT_NAME"]), strings.ToLower(rowUKCol["COLUMN_LIST"]))
			keysMeta = append(keysMeta, uk)
		}
	}
	if len(foreignKeyMap) > 0 {
		for _, rowFKCol := range foreignKeyMap {
			fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(`%s`) REFERENCES `%s`(`%s`)",
				strings.ToLower(rowFKCol["CONSTRAINT_NAME"]),
				strings.ToLower(rowFKCol["COLUMN_NAME"]),
				strings.ToLower(rowFKCol["RTABLE_NAME"]),
				strings.ToLower(rowFKCol["RCOLUMN_NAME"]))
			keysMeta = append(keysMeta, fk)
		}
	}
	if len(checkKeyMap) > 0 {
		for _, rowCKCol := range checkKeyMap {
			// 排除非空约束检查
			match, err := regexp.MatchString(`(^.*)(?i:IS NOT NULL)`, rowCKCol["SEARCH_CONDITION"])
			if err != nil {
				return keysMeta, fmt.Errorf("check constraint remove not null failed: %v", err)
			}
			if !match {
				ck := fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
					strings.ToLower(rowCKCol["CONSTRAINT_NAME"]),
					strings.ToLower(rowCKCol["SEARCH_CONDITION"]))
				keysMeta = append(keysMeta, ck)
			}
		}

	}
	return keysMeta, nil
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

func generateOracleTableColumnMetaByType(columnName, columnType string, rowCol map[string]string) string {
	var (
		nullable string
		colMeta  string
	)

	columnName = strings.ToLower(columnName)
	columnType = strings.ToLower(columnType)

	if rowCol["NULLABLE"] == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

	if nullable == "NULL" {
		switch {
		case rowCol["COMMENTS"] != "" && rowCol["DATA_DEFAULT"] != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s COMMENT '%s'", columnName, columnType, rowCol["DATA_DEFAULT"], rowCol["COMMENTS"])
		case rowCol["COMMENTS"] != "" && rowCol["DATA_DEFAULT"] == "":
			colMeta = fmt.Sprintf("`%s` %s COMMENT '%s'", columnName, columnType, rowCol["COMMENTS"])
		case rowCol["COMMENTS"] == "" && rowCol["DATA_DEFAULT"] != "":
			colMeta = fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, rowCol["DATA_DEFAULT"])
		case rowCol["COMMENTS"] == "" && rowCol["DATA_DEFAULT"] == "":
			colMeta = fmt.Sprintf("`%s` %s", columnName, columnType)
		}
	} else {
		switch {
		case rowCol["COMMENTS"] != "" && rowCol["DATA_DEFAULT"] != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT '%s'", columnName, columnType, nullable, rowCol["DATA_DEFAULT"], rowCol["COMMENTS"])
			return colMeta
		case rowCol["COMMENTS"] != "" && rowCol["DATA_DEFAULT"] == "":
			colMeta = fmt.Sprintf("`%s` %s %s COMMENT '%s'", columnName, columnType, nullable, rowCol["COMMENTS"])
		case rowCol["COMMENTS"] == "" && rowCol["DATA_DEFAULT"] != "":
			colMeta = fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, rowCol["DATA_DEFAULT"])
			return colMeta
		case rowCol["COMMENTS"] == "" && rowCol["DATA_DEFAULT"] == "":
			colMeta = fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable)
		}
	}
	return colMeta
}
