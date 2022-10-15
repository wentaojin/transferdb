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
	"strings"

	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
	"go.uber.org/zap"
)

type Table struct {
	MySQLDBType             string
	OracleDBVersion         string
	OracleExtendedMode      bool
	SourceSchemaName        string
	TargetSchemaName        string
	SourceTableName         string
	IsPartition             bool
	TargetTableName         string
	SourceTableCharacterSet string
	SourceTableCollation    string
	Overwrite               bool
	Engine                  *service.Engine `json:"-"`
}

func (t Table) GenCreateTableSQL() (string, []string, error) {
	var (
		createTableINFO []string
		createTableSQL  string
	)

	// 字段信息以及注释
	columnMetas, columnComments, err := t.CreateColumnMeta()
	if err != nil {
		return "", []string{}, fmt.Errorf("mysql db reverse table [%s] column meta and comments failed: %v", t.TargetTableName, err)
	}

	createTableINFO = append(createTableINFO, columnMetas...)

	// 约束
	ukINFO, err := t.GenCreatePKUKSQL()
	if err != nil {
		return "", []string{}, fmt.Errorf("mysql db reverse table [%s] primary and unique constraint failed: %v", t.TargetTableName, err)
	}

	createTableINFO = append(createTableINFO, ukINFO...)

	if !strings.EqualFold(t.MySQLDBType, utils.TiDBTargetDBType) {
		ckINFO, err := t.GenCreateCKSQL()
		if err != nil {
			return "", []string{}, fmt.Errorf("mysql db reverse table [%s] check constraint failed: %v", t.TargetTableName, err)
		}
		createTableINFO = append(createTableINFO, ckINFO...)
	} else {
		zap.L().Warn("reverse table mysql to oracle check constraint skip",
			zap.String("schema", t.SourceSchemaName),
			zap.String("table", t.SourceTableName),
			zap.String("mysql db type", t.MySQLDBType),
			zap.String("not support", "skip query"))
	}

	deleteFKINFO, updateFKINFO, err := t.GenCreateFKSQL()
	if err != nil {
		return "", []string{}, fmt.Errorf("mysql db reverse table [%s] foreign constraint failed: %v", t.TargetTableName, err)
	}

	createTableINFO = append(createTableINFO, deleteFKINFO...)
	createTableINFO = append(createTableINFO, updateFKINFO...)

	if !t.IsPartition {
		createTableSQL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s\n);", t.TargetSchemaName, t.TargetTableName, strings.Join(createTableINFO, ",\n"))
	} else {
		partitionINFO, err := t.Engine.GetMySQLPartitionTableDetailINFO(t.SourceSchemaName, t.SourceTableName)
		if err != nil {
			return createTableSQL, columnComments, err
		}
		createTableSQL = fmt.Sprintf("CREATE TABLE %s.%s (\n%s\n)\n PARTITION BY %s;",
			t.TargetSchemaName, t.TargetTableName, strings.Join(createTableINFO, ",\n"), partitionINFO)
	}

	return createTableSQL, columnComments, nil
}

func (t Table) GenCreatePKUKSQL() ([]string, error) {
	var keysMeta []string
	uniqueKeyMap, err := t.Engine.GetMySQLTablePrimaryAndUniqueKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return keysMeta, err
	}
	if len(uniqueKeyMap) > 0 {
		for _, rowUKCol := range uniqueKeyMap {
			var uk string
			if rowUKCol["CONSTRAINT_TYPE"] == "PK" {
				uk = fmt.Sprintf("PRIMARY KEY (%s)", strings.ToUpper(rowUKCol["COLUMN_LIST"]))
			} else {
				uk = fmt.Sprintf("CONSTRAINT %s UNIQUE (%s)",
					strings.ToUpper(rowUKCol["CONSTRAINT_NAME"]), strings.ToUpper(rowUKCol["COLUMN_LIST"]))
			}
			keysMeta = append(keysMeta, uk)
		}
	}
	return keysMeta, nil
}

func (t Table) GenCreateCKSQL() ([]string, error) {
	var ckMeta []string
	ckArrMap, err := t.Engine.GetMySQLTableCheckKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return []string{}, err
	}
	if len(ckArrMap) > 0 {
		for _, rowFKCol := range ckArrMap {
			ck := fmt.Sprintf("CONSTRAINT %s CHECK (%s)",
				strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
				strings.ToUpper(rowFKCol["SEARCH_CONDITION"]))
			ckMeta = append(ckMeta, ck)
		}
	}
	return ckMeta, nil
}

func (t Table) GenCreateFKSQL() ([]string, []string, error) {
	var (
		delFKMeta []string
		upFKMeta  []string
	)
	fkArrMap, err := t.Engine.GetMySQLTableForeignKey(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return delFKMeta, upFKMeta, err
	}
	if len(fkArrMap) > 0 {
		for _, rowFKCol := range fkArrMap {
			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" || rowFKCol["DELETE_RULE"] == "RESTRICT" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				delFKMeta = append(delFKMeta, fk)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON DELETE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				delFKMeta = append(delFKMeta, fk)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON DELETE SET NULL",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				delFKMeta = append(delFKMeta, fk)
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
				upFKMeta = append(upFKMeta, fk)
			}
			if rowFKCol["UPDATE_RULE"] == "CASCADE" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON UPDATE CASCADE",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				upFKMeta = append(upFKMeta, fk)
			}
			if rowFKCol["UPDATE_RULE"] == "SET NULL" {
				fk := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s (%s) ON UPDATE SET NULL",
					strings.ToUpper(rowFKCol["CONSTRAINT_NAME"]),
					strings.ToUpper(rowFKCol["COLUMN_LIST"]),
					strings.ToUpper(rowFKCol["R_OWNER"]),
					strings.ToUpper(rowFKCol["RTABLE_NAME"]),
					strings.ToUpper(rowFKCol["RCOLUMN_LIST"]))
				upFKMeta = append(upFKMeta, fk)
			}
		}
	}
	return delFKMeta, upFKMeta, nil
}

func (t Table) GenCreateIndexSQL() ([]string, error) {
	var indexMeta []string
	indexArrMap, err := t.Engine.GetMySQLTableIndex(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return []string{}, err
	}
	if len(indexArrMap) > 0 {
		for _, kv := range indexArrMap {
			var idx string
			if kv["UNIQUENESS"] == "UNIQUE" {
				idx = fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					t.TargetSchemaName,
					t.TargetTableName,
					strings.ToUpper(kv["COLUMN_LIST"]),
				)
			} else {
				idx = fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
					strings.ToUpper(kv["INDEX_NAME"]),
					t.TargetSchemaName,
					t.TargetTableName,
					strings.ToUpper(kv["COLUMN_LIST"]),
				)
			}
			indexMeta = append(indexMeta, idx)
		}
	}
	return indexMeta, nil
}

func (t Table) GenCreateTableCommentSQL() (string, error) {
	tableComment, err := t.Engine.GetMySQLTableComment(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return "", err
	}
	if tableComment != "" {
		return fmt.Sprintf(`COMMENT ON TABLE %s.%s IS '%s';`, t.TargetSchemaName, t.TargetTableName, tableComment), nil
	}
	return "", nil
}

func (t Table) CreateColumnMeta() ([]string, []string, error) {
	var (
		columnMetas    []string
		columnComments []string
	)
	columnsMap, err := t.Engine.GetMySQLTableColumn(t.SourceSchemaName, t.SourceTableName)
	if err != nil {
		return columnMetas, columnComments, err
	}

	for _, rowCol := range columnsMap {
		var (
			columnMeta, columnCollation string
			err                         error
		)

		if rowCol["COMMENTS"] != "" {
			columnComments = append(columnComments, fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS '%s';`, t.TargetSchemaName, t.TargetTableName, rowCol["COLUMN_NAME"], rowCol["COMMENTS"]))
		}

		// 字段排序规则检查
		if collationMapVal, ok := utils.MySQLDBCollationMap[strings.ToLower(rowCol["COLLATION_NAME"])]; ok {
			if utils.VersionOrdinal(t.OracleDBVersion) >= utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
				// oracle 12.2 版本及以上，字符集 collation 开启需激活 extended 特性
				// Collation BINARY_CS : Both case and accent sensitive. This is default if no extension is used.
				switch {
				case strings.EqualFold(rowCol["COLLATION_NAME"], "utf8mb4_bin") || strings.EqualFold(rowCol["COLLATION_NAME"], "utf8_bin"):
					collationArr := strings.Split(collationMapVal, "/")
					if t.OracleExtendedMode {
						columnCollation = collationArr[0]
					} else {
						// ORACLE 12.2 版本及以上非 extended 模式不支持设置字段 collation
						// columnCollation = collationArr[1]
						columnCollation = ""
					}
				default:
					columnCollation = collationMapVal
				}

			} else {
				// ORACLE 12.2 以下版本没有字段级别 collation，使用 oracledb 实例级别 collation
				columnCollation = ""
			}
		} else {

			switch {
			case !strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN"):
				return columnMetas, columnComments, fmt.Errorf(`mysql table column meta generate failed, column [%v] column type [%v] collation [%v], comment [%v], dataDefault [%v] nullable [%v]`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"], rowCol["COLLATION_NAME"], rowCol["COMMENTS"], rowCol["DATA_DEFAULT"], rowCol["NULLABLE"])
			case strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN"):
				// column collation value UNKNOWN, 代表是非字符串数据类型
				// skip ignore
				columnCollation = ""
			default:
				return columnComments, columnComments, fmt.Errorf("mysql table column meta generate failed, column [%s] not support collation [%s]", rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"])
			}
		}

		columnMeta, err = ReverseMySQLTableColumnMapRule(
			t.SourceSchemaName,
			t.SourceTableName,
			rowCol["COLUMN_NAME"],
			rowCol["DATA_TYPE"],
			rowCol["NULLABLE"],
			"", // Comment 单独处理
			rowCol["DATA_DEFAULT"],
			rowCol["DATA_SCALE"],
			rowCol["DATA_PRECISION"],
			rowCol["DATA_LENGTH"],
			rowCol["DATETIME_PRECISION"],
			columnCollation,
			t.Engine,
		)

		if err != nil {
			return columnMetas, columnComments, err
		}

		columnMetas = append(columnMetas, columnMeta)
	}

	return columnMetas, columnComments, nil
}

func (t *Table) String() string {
	jsonStr, _ := json.Marshal(t)
	return string(jsonStr)
}
