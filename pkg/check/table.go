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
package check

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"
)

type Table struct {
	SchemaName         string
	TableName          string
	TableComment       string
	TableCharacterSet  string
	TableCollation     string
	Columns            map[string]Column // KEY 字段名
	Indexes            []Index
	PUConstraints      []ConstraintPUKey
	ForeignConstraints []ConstraintForeign
	CheckConstraints   []ConstraintCheck
	IsPartition        bool
	Partitions         []Partition // 获取分区键、分区类型以及子分区键、子分区类型
}

type Column struct {
	DataType                string
	CharLength              string
	CharUsed                string
	CharacterSet            string
	Collation               string
	OracleOriginDataDefault string
	MySQLOriginDataDefault  string
	ColumnInfo
}

type ColumnInfo struct {
	DataLength        string
	DataPrecision     string
	DataScale         string
	DatetimePrecision string
	NULLABLE          string
	DataDefault       string
	Comment           string
}

type Index struct {
	IndexInfo
	IndexType     string
	ColumnExpress string
}

type IndexInfo struct {
	Uniqueness  string
	IndexColumn string
}

type ConstraintPUKey struct {
	ConstraintType   string
	ConstraintColumn string
}

type ConstraintForeign struct {
	ColumnName            string
	ReferencedTableSchema string
	ReferencedTableName   string
	ReferencedColumnName  string
	DeleteRule            string
	UpdateRule            string
}

type ConstraintCheck struct {
	ConstraintExpression string
}

type Partition struct {
	PartitionKey     string
	PartitionType    string
	SubPartitionKey  string
	SubPartitionType string
}

func (t *Table) String(jsonType string) string {
	var jsonStr []byte
	switch jsonType {
	case utils.ColumnsJSON:
		jsonStr, _ = json.Marshal(t.Columns)
	case utils.PUConstraintJSON:
		jsonStr, _ = json.Marshal(t.PUConstraints)
	case utils.FKConstraintJSON:
		jsonStr, _ = json.Marshal(t.ForeignConstraints)
	case utils.CKConstraintJSON:
		jsonStr, _ = json.Marshal(t.CheckConstraints)
	case utils.IndexJSON:
		jsonStr, _ = json.Marshal(t.Indexes)
	case utils.PartitionJSON:
		jsonStr, _ = json.Marshal(t.Partitions)
	}
	return string(jsonStr)
}

func NewOracleTableINFO(schemaName, tableName string, engine *service.Engine) (*Table, error) {
	oraTable := &Table{
		SchemaName: schemaName,
		TableName:  tableName,
	}
	isGBKCharacterSet := false

	characterSet, err := engine.GetOracleDBCharacterSet()
	if err != nil {
		return oraTable, err
	}
	if strings.Contains(strings.ToUpper(characterSet), ".ZHS16GBK") {
		isGBKCharacterSet = true
	}
	commentInfo, err := engine.GetOracleTableComment(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	oraTable.TableComment = strings.ToUpper(commentInfo[0]["COMMENTS"])

	columnInfo, err := engine.GetOracleTableColumn(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	columns := make(map[string]Column, len(columnInfo))
	var OracleCharacterSet string

	for _, rowCol := range columnInfo {
		if isGBKCharacterSet {
			OracleCharacterSet = utils.OracleUTF8CharacterSet
		} else {
			OracleCharacterSet = utils.OracleGBKCharacterSet
		}
		var (
			nullable    string
			dataDefault string
		)
		if strings.ToUpper(rowCol["NULLABLE"]) == "Y" {
			nullable = "NULL"
		} else {
			nullable = "NOT NULL"
		}

		dataDefault = strings.TrimSpace(rowCol["DATA_DEFAULT"])

		if strings.HasPrefix(dataDefault, "'") {
			dataDefault = strings.TrimPrefix(dataDefault, "'")
		}

		if strings.HasSuffix(dataDefault, "'") {
			dataDefault = strings.TrimSuffix(dataDefault, "'")
		}

		columns[strings.ToUpper(rowCol["COLUMN_NAME"])] = Column{
			DataType:   strings.ToUpper(rowCol["DATA_TYPE"]),
			CharLength: strings.ToUpper(rowCol["CHAR_LENGTH"]),
			CharUsed:   strings.ToUpper(rowCol["CHAR_USED"]),
			ColumnInfo: ColumnInfo{
				DataLength:        strings.ToUpper(rowCol["DATA_LENGTH"]),
				DataPrecision:     strings.ToUpper(rowCol["DATA_PRECISION"]),
				DataScale:         strings.ToUpper(rowCol["DATA_SCALE"]),
				DatetimePrecision: "", // only mysql
				NULLABLE:          nullable,
				DataDefault:       dataDefault,
				Comment:           strings.ToUpper(rowCol["COMMENTS"]),
			},
			CharacterSet:            strings.ToUpper(OracleCharacterSet),
			Collation:               strings.ToUpper(utils.OracleCollationBin),
			OracleOriginDataDefault: strings.TrimSpace(rowCol["DATA_DEFAULT"]),
			MySQLOriginDataDefault:  "", // only mysql
		}
	}

	var indexes []Index

	indexInfo, err := engine.GetOracleTableNormalIndex(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, indexCol := range indexInfo {
		indexes = append(indexes, Index{
			IndexInfo: IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexType:     strings.ToUpper(indexCol["INDEX_TYPE"]),
			ColumnExpress: strings.ToUpper(indexCol["COLUMN_EXPRESSION"]),
		})
	}

	indexInfo, err = engine.GetOracleTableUniqueIndex(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, indexCol := range indexInfo {
		indexes = append(indexes, Index{
			IndexInfo: IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexType:     strings.ToUpper(indexCol["INDEX_TYPE"]),
			ColumnExpress: strings.ToUpper(indexCol["COLUMN_EXPRESSION"]),
		})
	}

	var puConstraints []ConstraintPUKey
	pkInfo, err := engine.GetOracleTablePrimaryKey(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, pk := range pkInfo {
		puConstraints = append(puConstraints, ConstraintPUKey{
			ConstraintType:   "PK",
			ConstraintColumn: strings.ToUpper(pk["COLUMN_LIST"]),
		})
	}

	ukInfo, err := engine.GetOracleTableUniqueKey(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, pk := range ukInfo {
		puConstraints = append(puConstraints, ConstraintPUKey{
			ConstraintType:   "UK",
			ConstraintColumn: strings.ToUpper(pk["COLUMN_LIST"]),
		})
	}

	var fkConstraints []ConstraintForeign
	fkInfo, err := engine.GetOracleTableForeignKey(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, fk := range fkInfo {
		fkConstraints = append(fkConstraints, ConstraintForeign{
			ColumnName:            strings.ToUpper(fk["COLUMN_LIST"]),
			ReferencedTableSchema: strings.ToUpper(fk["R_OWNER"]),
			ReferencedTableName:   strings.ToUpper(fk["RTABLE_NAME"]),
			ReferencedColumnName:  strings.ToUpper(fk["RCOLUMN_LIST"]),
			DeleteRule:            strings.ToUpper(fk["DELETE_RULE"]),
			UpdateRule:            "", // Oracle 不支持 Update Rule
		})
	}
	var ckConstraints []ConstraintCheck
	ckInfo, err := engine.GetOracleTableCheckKey(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, ck := range ckInfo {
		ckConstraints = append(ckConstraints, ConstraintCheck{
			ConstraintExpression: strings.ToUpper(ck["SEARCH_CONDITION"]),
		})
	}
	isPart, err := engine.IsOraclePartitionTable(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	var parts []Partition
	if isPart {
		partInfo, err := engine.GetOraclePartitionTableINFO(schemaName, tableName)
		if err != nil {
			return oraTable, err
		}
		for _, part := range partInfo {
			parts = append(parts, Partition{
				PartitionKey:     strings.ToUpper(part["PARTITION_EXPRESS"]),
				PartitionType:    strings.ToUpper(part["PARTITIONING_TYPE"]),
				SubPartitionKey:  strings.ToUpper(part["SUBPARTITION_EXPRESS"]),
				SubPartitionType: strings.ToUpper(part["SUBPARTITIONING_TYPE"]),
			})
		}
	}
	oraTable.TableCharacterSet = strings.ToUpper(OracleCharacterSet)
	oraTable.TableCollation = utils.OracleCollationBin
	oraTable.Columns = columns
	oraTable.Indexes = indexes
	oraTable.PUConstraints = puConstraints
	oraTable.ForeignConstraints = fkConstraints
	oraTable.CheckConstraints = ckConstraints
	oraTable.IsPartition = isPart
	oraTable.Partitions = parts
	return oraTable, nil
}

func NewMySQLTableINFO(schemaName, tableName string, engine *service.Engine) (*Table, string, error) {
	mysqlTable := &Table{
		SchemaName: schemaName,
		TableName:  tableName,
	}

	version, err := engine.GetMySQLDBVersion()
	if err != nil {
		return mysqlTable, version, err
	}

	// 是否 TiDB 版本
	isTiDB := false
	if strings.Contains(version, "TiDB") {
		isTiDB = true
	}

	var (
		characterSet, collation string
	)

	isExist, err := engine.IsExistMySQLTableCharacterSetAndCollation(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}

	if isExist {
		characterSet, collation, err = engine.GetMySQLTableCharacterSetAndCollation(schemaName, tableName)
		if err != nil {
			return mysqlTable, version, err
		}
	}

	if characterSet == "UNKNOWN" || collation == "UNKNOWN" || characterSet == "" || collation == "" {
		characterSet, err = engine.GetMySQLDBServerCharacterSet()
		if err != nil {
			return mysqlTable, version, err
		}
		collation, err = engine.GetMySQLDBServerCollation()
		if err != nil {
			return mysqlTable, version, err
		}
	}

	comment, err := engine.GetMySQLTableComment(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}
	mysqlTable.TableComment = strings.ToUpper(comment)

	columnInfo, err := engine.GetMySQLTableColumn(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}
	columns := make(map[string]Column, len(columnInfo))

	for _, rowCol := range columnInfo {
		var (
			nullable    string
			dataDefault string
		)
		if strings.ToUpper(rowCol["NULLABLE"]) == "Y" {
			nullable = "NULL"
		} else {
			nullable = "NOT NULL"
		}

		// 修复 mysql 默认值存在单引号问题
		// oracle 单引号默认值 '''PC''' , mysql 单引号默认值 'PC'
		// 对比 oracle 会去掉前后单引号, mysql 增加前后单引号
		dataDefault = strings.TrimSpace(rowCol["DATA_DEFAULT"])

		if strings.HasPrefix(dataDefault, "'") {
			dataDefault = fmt.Sprintf("'%s", dataDefault)
		}

		if strings.HasSuffix(dataDefault, "'") {
			dataDefault = fmt.Sprintf("%s'", dataDefault)
		}

		columns[strings.ToUpper(rowCol["COLUMN_NAME"])] = Column{
			DataType: strings.ToUpper(rowCol["DATA_TYPE"]),
			ColumnInfo: ColumnInfo{
				DataLength:        strings.ToUpper(rowCol["DATA_LENGTH"]),
				DataPrecision:     strings.ToUpper(rowCol["DATA_PRECISION"]),
				DataScale:         strings.ToUpper(rowCol["DATA_SCALE"]),
				DatetimePrecision: strings.ToUpper(rowCol["DATETIME_PRECISION"]),
				NULLABLE:          nullable,
				DataDefault:       dataDefault,
				Comment:           strings.ToUpper(rowCol["COMMENTS"]),
			},
			CharacterSet:            strings.ToUpper(rowCol["CHARACTER_SET_NAME"]),
			Collation:               strings.ToUpper(rowCol["COLLATION_NAME"]),
			OracleOriginDataDefault: "", // only oracle
			MySQLOriginDataDefault:  strings.TrimSpace(rowCol["DATA_DEFAULT"]),
		}
	}

	indexInfo, err := engine.GetMySQLTableIndex(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}
	var indexes []Index
	for _, indexCol := range indexInfo {
		indexes = append(indexes, Index{
			IndexInfo: IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexType:     strings.ToUpper(indexCol["INDEX_TYPE"]),
			ColumnExpress: strings.ToUpper(indexCol["COLUMN_EXPRESSION"]),
		})
	}

	var puConstraints []ConstraintPUKey
	puInfo, err := engine.GetMySQLTablePrimaryAndUniqueKey(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}
	for _, pu := range puInfo {
		puConstraints = append(puConstraints, ConstraintPUKey{
			ConstraintType:   strings.ToUpper(pu["CONSTRAINT_TYPE"]),
			ConstraintColumn: strings.ToUpper(pu["COLUMN_LIST"]),
		})
	}

	var (
		ckConstraints []ConstraintCheck
		fkConstraints []ConstraintForeign
		parts         []Partition
	)
	isPart, err := engine.IsMySQLPartitionTable(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}
	if isPart {
		partInfo, err := engine.GetMySQLPartitionTableINFO(schemaName, tableName)
		if err != nil {
			return mysqlTable, version, err
		}
		for _, part := range partInfo {
			parts = append(parts, Partition{
				PartitionKey:     strings.ToUpper(part["PARTITION_EXPRESS"]),
				PartitionType:    strings.ToUpper(part["PARTITIONING_TYPE"]),
				SubPartitionKey:  strings.ToUpper(part["SUBPARTITION_EXPRESS"]),
				SubPartitionType: strings.ToUpper(part["SUBPARTITIONING_TYPE"]),
			})
		}
	}

	if !isTiDB {
		fkInfo, err := engine.GetMySQLTableForeignKey(schemaName, tableName)
		if err != nil {
			return mysqlTable, version, err
		}
		for _, fk := range fkInfo {
			fkConstraints = append(fkConstraints, ConstraintForeign{
				ColumnName:            strings.ToUpper(fk["COLUMN_LIST"]),
				ReferencedTableSchema: strings.ToUpper(fk["R_OWNER"]),
				ReferencedTableName:   strings.ToUpper(fk["RTABLE_NAME"]),
				ReferencedColumnName:  strings.ToUpper(fk["RCOLUMN_LIST"]),
				DeleteRule:            strings.ToUpper(fk["DELETE_RULE"]),
				UpdateRule:            strings.ToUpper(fk["UPDATE_RULE"]),
			})
		}

		var dbVersion string
		if strings.Contains(version, utils.MySQLVersionDelimiter) {
			dbVersion = strings.Split(version, utils.MySQLVersionDelimiter)[0]
		} else {
			dbVersion = version
		}
		if utils.VersionOrdinal(dbVersion) > utils.VersionOrdinal(utils.MySQLCheckConsVersion) {
			ckInfo, err := engine.GetMySQLTableCheckKey(schemaName, tableName)
			if err != nil {
				return mysqlTable, version, err
			}
			for _, ck := range ckInfo {
				ckConstraints = append(ckConstraints, ConstraintCheck{
					ConstraintExpression: strings.ToUpper(ck["SEARCH_CONDITION"]),
				})
			}
		}
	}

	mysqlTable.TableCharacterSet = strings.ToUpper(characterSet)
	mysqlTable.TableCollation = strings.ToUpper(collation)
	mysqlTable.Columns = columns
	mysqlTable.Indexes = indexes
	mysqlTable.PUConstraints = puConstraints
	mysqlTable.ForeignConstraints = fkConstraints
	mysqlTable.CheckConstraints = ckConstraints
	mysqlTable.IsPartition = isPart
	mysqlTable.Partitions = parts
	return mysqlTable, version, nil
}

func generateColumnNullCommentDefaultMeta(dataNullable, comments, dataDefault string) string {
	var (
		colMeta string
	)

	if dataNullable == "NULL" {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("DEFAULT %s COMMENT '%s'", dataDefault, comments)
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("DEFAULT NULL COMMENT '%s'", comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("DEFAULT %s", dataDefault)
		case comments == "" && dataDefault == "":
			colMeta = "DEFAULT NULL"
		}
	} else {
		switch {
		case comments != "" && dataDefault != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s COMMENT '%s'", dataNullable, dataDefault, comments)
		case comments != "" && dataDefault == "":
			colMeta = fmt.Sprintf("%s COMMENT '%s'", dataNullable, comments)
		case comments == "" && dataDefault != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s", dataNullable, dataDefault)
		case comments == "" && dataDefault == "":
			colMeta = fmt.Sprintf("%s", dataNullable)
		}
	}
	return colMeta
}
