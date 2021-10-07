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
	"strings"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"
)

const (
	OracleGBKCharacterSet  = "gbk"
	OracleUTF8CharacterSet = "utf8"
	// oracle collation 默认大小写敏感，a != A
	OracleCollationBin = "bin"
	// MySQL 支持 check 约束版本 > 8.0.15
	MySQLCheckConsVersion = "8.0.15"
	// MySQL 版本分隔符号
	MySQLVersionDelimiter = "-"
	// MySQL 字符集/排序规则
	MySQLCharacterSet = "utf8mb4"
	MySQLCollation    = "utf8mb4_bin"
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
	DataType     string
	CharLength   string
	CharUsed     string
	CharacterSet string
	Collation    string
	ColumnInfo
}

type ColumnInfo struct {
	DataLength    string
	DataPrecision string
	DataScale     string
	NULLABLE      string
	DataDefault   string
	Comment       string
}

type Index struct {
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
	oraTable.TableComment = commentInfo[0]["COMMENTS"]

	columnInfo, err := engine.GetOracleTableColumn(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	columns := make(map[string]Column, len(columnInfo))
	var OracleCharacterSet string

	for _, rowCol := range columnInfo {
		if isGBKCharacterSet {
			OracleCharacterSet = OracleUTF8CharacterSet
		} else {
			OracleCharacterSet = OracleGBKCharacterSet
		}
		columns[rowCol["COLUMN_NAME"]] = Column{
			DataType:   rowCol["DATA_TYPE"],
			CharLength: rowCol["CHAR_LENGTH"],
			CharUsed:   rowCol["CHAR_USED"],
			ColumnInfo: ColumnInfo{
				DataLength:    rowCol["DATA_LENGTH"],
				DataPrecision: rowCol["DATA_PRECISION"],
				DataScale:     rowCol["DATA_SCALE"],
				NULLABLE:      rowCol["NULLABLE"],
				DataDefault:   rowCol["DATA_DEFAULT"],
				Comment:       rowCol["COMMENTS"],
			},
			CharacterSet: OracleCharacterSet,
			Collation:    OracleCollationBin,
		}
	}

	indexInfo, err := engine.GetOracleTableIndex(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	var indexes []Index
	for _, indexCol := range indexInfo {
		indexes = append(indexes, Index{
			Uniqueness:  indexCol["UNIQUENESS"],
			IndexColumn: indexCol["COLUMN_LIST"],
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
			ConstraintColumn: pk["COLUMN_LIST"],
		})
	}

	ukInfo, err := engine.GetOracleTableUniqueKey(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, pk := range ukInfo {
		puConstraints = append(puConstraints, ConstraintPUKey{
			ConstraintType:   "UK",
			ConstraintColumn: pk["COLUMN_LIST"],
		})
	}

	var fkConstraints []ConstraintForeign
	fkInfo, err := engine.GetOracleTableForeignKey(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	for _, fk := range fkInfo {
		fkConstraints = append(fkConstraints, ConstraintForeign{
			ColumnName:            fk["COLUMN_LIST"],
			ReferencedTableSchema: fk["R_OWNER"],
			ReferencedTableName:   fk["RTABLE_NAME"],
			ReferencedColumnName:  fk["RCOLUMN_LIST"],
			DeleteRule:            fk["DELETE_RULE"],
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
			ConstraintExpression: ck["SEARCH_CONDITION"],
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
				PartitionKey:     part["PARTITION_EXPRESS"],
				PartitionType:    part["PARTITIONING_TYPE"],
				SubPartitionKey:  part["SUBPARTITION_EXPRESS"],
				SubPartitionType: part["SUBPARTITIONING_TYPE"],
			})
		}
	}
	oraTable.TableCharacterSet = OracleCharacterSet
	oraTable.TableCollation = OracleCollationBin
	oraTable.Columns = columns
	oraTable.Indexes = indexes
	oraTable.PUConstraints = puConstraints
	oraTable.ForeignConstraints = fkConstraints
	oraTable.CheckConstraints = ckConstraints
	oraTable.IsPartition = isPart
	oraTable.Partitions = parts
	return oraTable, nil
}

func NewMySQLTableINFO(schemaName, tableName string, engine *service.Engine) (*Table, error) {
	mysqlTable := &Table{
		SchemaName: schemaName,
		TableName:  tableName,
	}
	characterSet, collation, err := engine.GetMySQLTableCharacterSetAndCollation(schemaName, tableName)
	if err != nil {
		return mysqlTable, err
	}

	version, err := engine.GetMySQLDBVersion()
	if err != nil {
		return mysqlTable, err
	}
	isTiDB := false
	if strings.Contains(version, "TiDB") {
		isTiDB = true
	}

	comment, err := engine.GetMySQLTableComment(schemaName, tableName)
	if err != nil {
		return mysqlTable, err
	}
	mysqlTable.TableComment = comment

	columnInfo, err := engine.GetMySQLTableColumn(schemaName, tableName)
	if err != nil {
		return mysqlTable, err
	}
	columns := make(map[string]Column, len(columnInfo))

	for _, rowCol := range columnInfo {
		var cs, ca string
		if rowCol["CHARACTER_SET_NAME"] != characterSet {
			cs = rowCol["CHARACTER_SET_NAME"]
		} else {
			cs = characterSet
		}
		if rowCol["COLLATION_NAME"] != collation {
			ca = rowCol["COLLATION_NAME"]
		} else {
			ca = collation
		}
		columns[rowCol["COLUMN_NAME"]] = Column{
			DataType: rowCol["DATA_TYPE"],
			ColumnInfo: ColumnInfo{
				DataLength:    rowCol["DATA_LENGTH"],
				DataPrecision: rowCol["DATA_PRECISION"],
				DataScale:     rowCol["DATA_SCALE"],
				NULLABLE:      rowCol["NULLABLE"],
				DataDefault:   rowCol["DATA_DEFAULT"],
				Comment:       rowCol["COMMENTS"],
			},
			CharacterSet: cs,
			Collation:    ca,
		}
	}

	indexInfo, err := engine.GetMySQLTableIndex(schemaName, tableName)
	if err != nil {
		return mysqlTable, err
	}
	var indexes []Index
	for _, indexCol := range indexInfo {
		indexes = append(indexes, Index{
			Uniqueness:  indexCol["UNIQUENESS"],
			IndexColumn: indexCol["COLUMN_LIST"],
		})
	}

	var puConstraints []ConstraintPUKey
	puInfo, err := engine.GetMySQLTablePrimaryAndUniqueKey(schemaName, tableName)
	if err != nil {
		return mysqlTable, err
	}
	for _, pu := range puInfo {
		puConstraints = append(puConstraints, ConstraintPUKey{
			ConstraintType:   pu["CONSTRAINT_TYPE"],
			ConstraintColumn: pu["COLUMN_LIST"],
		})
	}

	var (
		ckConstraints []ConstraintCheck
		fkConstraints []ConstraintForeign
		parts         []Partition
	)
	isPart, err := engine.IsMySQLPartitionTable(schemaName, tableName)
	if err != nil {
		return mysqlTable, err
	}
	if isPart {
		partInfo, err := engine.GetMySQLPartitionTableINFO(schemaName, tableName)
		if err != nil {
			return mysqlTable, err
		}
		for _, part := range partInfo {
			parts = append(parts, Partition{
				PartitionKey:     part["PARTITION_EXPRESS"],
				PartitionType:    part["PARTITIONING_TYPE"],
				SubPartitionKey:  part["SUBPARTITION_EXPRESS"],
				SubPartitionType: part["SUBPARTITIONING_TYPE"],
			})
		}
	}

	if !isTiDB {
		fkInfo, err := engine.GetMySQLTableForeignKey(schemaName, tableName)
		if err != nil {
			return mysqlTable, err
		}
		for _, fk := range fkInfo {
			fkConstraints = append(fkConstraints, ConstraintForeign{
				ColumnName:            fk["COLUMN_LIST"],
				ReferencedTableSchema: fk["R_OWNER"],
				ReferencedTableName:   fk["RTABLE_NAME"],
				ReferencedColumnName:  fk["RCOLUMN_LIST"],
				DeleteRule:            fk["DELETE_RULE"],
				UpdateRule:            fk["UPDATE_RULE"],
			})
		}

		var dbVersion string
		if strings.Contains(version, MySQLVersionDelimiter) {
			dbVersion = strings.Split(version, MySQLVersionDelimiter)[0]
		} else {
			dbVersion = version
		}
		if utils.VersionOrdinal(dbVersion) > utils.VersionOrdinal(MySQLCheckConsVersion) {
			ckInfo, err := engine.GetMySQLTableCheckKey(schemaName, tableName)
			if err != nil {
				return mysqlTable, err
			}
			for _, ck := range ckInfo {
				ckConstraints = append(ckConstraints, ConstraintCheck{
					ConstraintExpression: ck["SEARCH_CONDITION"],
				})
			}
		}
	}

	mysqlTable.TableCharacterSet = characterSet
	mysqlTable.TableCollation = collation
	mysqlTable.Columns = columns
	mysqlTable.Indexes = indexes
	mysqlTable.PUConstraints = puConstraints
	mysqlTable.ForeignConstraints = fkConstraints
	mysqlTable.CheckConstraints = ckConstraints
	mysqlTable.IsPartition = isPart
	mysqlTable.Partitions = parts
	return mysqlTable, nil
}
