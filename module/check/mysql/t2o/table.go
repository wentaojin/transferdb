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
package t2o

import (
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/check/mysql/public"
	"strings"
)

/*
	Oracle
*/

func NewOracleTableINFO(schemaName, tableName string, oracle *oracle.Oracle, sourceCharacterSet, nlsComp string,
	sourceTableCollation, sourceSchemaCollation string, oracleCollation bool) (*public.Table, error) {
	oraTable := &public.Table{
		SchemaName: schemaName,
		TableName:  tableName,
	}
	// table collation
	tableCollation, err := genTableCollation(nlsComp, oracleCollation, sourceSchemaCollation, sourceTableCollation)
	if err != nil {
		return oraTable, err
	}

	commentInfo, err := oracle.GetOracleSchemaTableComment(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	oraTable.TableComment = strings.ToUpper(commentInfo[0]["COMMENTS"])

	columns, OracleCharacterSet, err := GetOracleTableColumn(schemaName, tableName, oracle,
		strings.Split(sourceCharacterSet, ".")[1], nlsComp, sourceTableCollation, sourceSchemaCollation, oracleCollation)
	if err != nil {
		return oraTable, err
	}

	indexes, err := GetOracleTableIndex(schemaName, tableName, oracle)
	if err != nil {
		return oraTable, err
	}

	puConstraints, fkConstraints, ckConstraints, err := GetOracleConstraint(schemaName, tableName, oracle)
	if err != nil {
		return oraTable, err
	}

	isPart, err := oracle.IsOraclePartitionTable(schemaName, tableName)
	if err != nil {
		return oraTable, err
	}
	var parts []public.Partition
	if isPart {
		partInfo, err := oracle.GetOraclePartitionTableINFO(schemaName, tableName)
		if err != nil {
			return oraTable, err
		}
		for _, part := range partInfo {
			parts = append(parts, public.Partition{
				PartitionKey:     strings.ToUpper(part["PARTITION_EXPRESS"]),
				PartitionType:    strings.ToUpper(part["PARTITIONING_TYPE"]),
				SubPartitionKey:  strings.ToUpper(part["SUBPARTITION_EXPRESS"]),
				SubPartitionType: strings.ToUpper(part["SUBPARTITIONING_TYPE"]),
			})
		}
	}
	oraTable.TableCharacterSet = strings.ToUpper(OracleCharacterSet)
	oraTable.TableCollation = tableCollation
	oraTable.Columns = columns
	oraTable.Indexes = indexes
	oraTable.PUConstraints = puConstraints
	oraTable.ForeignConstraints = fkConstraints
	oraTable.CheckConstraints = ckConstraints
	oraTable.IsPartition = isPart
	oraTable.Partitions = parts
	return oraTable, nil
}

func GetOracleTableColumn(schemaName, tableName string, oracle *oracle.Oracle, sourceDBCharacterSet, nlsComp string,
	sourceTableCollation string, sourceSchemaCollation string, oraCollation bool) (map[string]public.Column, string, error) {
	columnInfo, err := oracle.GetOracleSchemaTableColumn(schemaName, tableName, oraCollation)
	if err != nil {
		return nil, "", err
	}

	columns := make(map[string]public.Column, len(columnInfo))

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

		dataDefault = strings.TrimSpace(rowCol["DATA_DEFAULT"])

		// 处理 oracle 默认值 ('xxx') 或者 (xxx)
		if strings.HasPrefix(dataDefault, "(") && strings.HasSuffix(dataDefault, ")") {
			dataDefault = strings.TrimLeft(dataDefault, "(")
			dataDefault = strings.TrimRight(dataDefault, ")")
		}

		// 修复 mysql 默认值存在单引号问题
		// oracle 单引号默认值 '''PC''' , mysql 单引号默认值 'PC'
		// 对比 oracle 会去掉前后一个单引号 ''PC'', mysql 增加前后单引号 ''PC''
		if strings.HasPrefix(dataDefault, "'") {
			dataDefault = strings.TrimPrefix(dataDefault, "'")
		}

		if strings.HasSuffix(dataDefault, "'") {
			dataDefault = strings.TrimSuffix(dataDefault, "'")
		}

		column := public.Column{
			DataType:   strings.ToUpper(rowCol["DATA_TYPE"]),
			CharLength: strings.ToUpper(rowCol["CHAR_LENGTH"]),
			CharUsed:   strings.ToUpper(rowCol["CHAR_USED"]),
			ColumnInfo: public.ColumnInfo{
				DataLength:        strings.ToUpper(rowCol["DATA_LENGTH"]),
				DataPrecision:     strings.ToUpper(rowCol["DATA_PRECISION"]),
				DataScale:         strings.ToUpper(rowCol["DATA_SCALE"]),
				DatetimePrecision: "", // only mysql
				NULLABLE:          nullable,
				DataDefault:       dataDefault,
				Comment:           strings.ToUpper(rowCol["COMMENTS"]),
			},
			CharacterSet:            strings.ToUpper(sourceDBCharacterSet), // oracle db characterSet only one
			OracleOriginDataDefault: strings.TrimSpace(rowCol["DATA_DEFAULT"]),
			MySQLOriginDataDefault:  "", // only mysql
		}

		columnCollation, err := genTableColumnCollation(nlsComp, oraCollation, sourceSchemaCollation, sourceTableCollation, strings.ToUpper(rowCol["COLLATION"]))
		if err != nil {
			return columns, "", err
		}
		column.Collation = columnCollation

		columns[strings.ToUpper(rowCol["COLUMN_NAME"])] = column
	}
	return columns, strings.ToUpper(sourceDBCharacterSet), err
}

func GetOracleTableIndex(schemaName, tableName string, oracle *oracle.Oracle) ([]public.Index, error) {
	var indexes []public.Index

	indexInfo, err := oracle.GetOracleSchemaTableNormalIndex(schemaName, tableName)
	if err != nil {
		return indexes, err
	}
	for _, indexCol := range indexInfo {
		indexes = append(indexes, public.Index{
			IndexInfo: public.IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexName:        strings.ToUpper(indexCol["INDEX_NAME"]),
			IndexType:        strings.ToUpper(indexCol["INDEX_TYPE"]),
			DomainIndexOwner: strings.ToUpper(indexCol["ITYP_OWNER"]),
			DomainIndexName:  strings.ToUpper(indexCol["ITYP_NAME"]),
			DomainParameters: strings.ToUpper(indexCol["PARAMETERS"]),
		})
	}

	indexInfo, err = oracle.GetOracleSchemaTableUniqueIndex(schemaName, tableName)
	if err != nil {
		return indexes, err
	}
	for _, indexCol := range indexInfo {
		indexes = append(indexes, public.Index{
			IndexInfo: public.IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexName:        strings.ToUpper(indexCol["INDEX_NAME"]),
			IndexType:        strings.ToUpper(indexCol["INDEX_TYPE"]),
			DomainIndexOwner: strings.ToUpper(indexCol["ITYP_OWNER"]),
			DomainIndexName:  strings.ToUpper(indexCol["ITYP_NAME"]),
			DomainParameters: strings.ToUpper(indexCol["PARAMETERS"]),
		})
	}

	return indexes, nil
}

func GetOracleConstraint(schemaName, tableName string, oracle *oracle.Oracle) ([]public.ConstraintPUKey, []public.ConstraintForeign, []public.ConstraintCheck, error) {
	var (
		puConstraints []public.ConstraintPUKey
		fkConstraints []public.ConstraintForeign
		ckConstraints []public.ConstraintCheck
	)

	pkInfo, err := oracle.GetOracleSchemaTablePrimaryKey(schemaName, tableName)
	if err != nil {
		return puConstraints, fkConstraints, ckConstraints, err
	}
	for _, pk := range pkInfo {
		puConstraints = append(puConstraints, public.ConstraintPUKey{
			ConstraintType:   "PK",
			ConstraintColumn: strings.ToUpper(pk["COLUMN_LIST"]),
		})
	}

	ukInfo, err := oracle.GetOracleSchemaTableUniqueKey(schemaName, tableName)
	if err != nil {
		return puConstraints, fkConstraints, ckConstraints, err
	}
	for _, pk := range ukInfo {
		puConstraints = append(puConstraints, public.ConstraintPUKey{
			ConstraintType:   "UK",
			ConstraintColumn: strings.ToUpper(pk["COLUMN_LIST"]),
		})
	}

	fkInfo, err := oracle.GetOracleSchemaTableForeignKey(schemaName, tableName)
	if err != nil {
		return puConstraints, fkConstraints, ckConstraints, err
	}
	for _, fk := range fkInfo {
		fkConstraints = append(fkConstraints, public.ConstraintForeign{
			ColumnName:            strings.ToUpper(fk["COLUMN_LIST"]),
			ReferencedTableSchema: strings.ToUpper(fk["R_OWNER"]),
			ReferencedTableName:   strings.ToUpper(fk["RTABLE_NAME"]),
			ReferencedColumnName:  strings.ToUpper(fk["RCOLUMN_LIST"]),
			DeleteRule:            strings.ToUpper(fk["DELETE_RULE"]),
			UpdateRule:            "", // Oracle 不支持 Update Rule
		})
	}

	ckInfo, err := oracle.GetOracleSchemaTableCheckKey(schemaName, tableName)
	if err != nil {
		return puConstraints, fkConstraints, ckConstraints, err
	}
	for _, ck := range ckInfo {
		ckConstraints = append(ckConstraints, public.ConstraintCheck{
			ConstraintExpression: strings.ToUpper(ck["SEARCH_CONDITION"]),
		})
	}
	return puConstraints, fkConstraints, ckConstraints, nil
}

/*
	MySQL
*/

func NewMySQLTableINFO(schemaName, tableName, targetDBType string, mysql *mysql.MySQL) (*public.Table, string, error) {
	mysqlTable := &public.Table{
		SchemaName: schemaName,
		TableName:  tableName,
	}

	version, err := mysql.GetMySQLDBVersion()
	if err != nil {
		return mysqlTable, version, err
	}

	// 是否 TiDB 版本
	isTiDB := false
	if strings.EqualFold(targetDBType, common.DatabaseTypeTiDB) {
		isTiDB = true
	}

	var (
		characterSet, collation string
	)

	isExist, err := mysql.IsExistMySQLTableCharacterSetAndCollation(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}

	if isExist {
		characterSet, collation, err = mysql.GetMySQLTableCharacterSetAndCollation(schemaName, tableName)
		if err != nil {
			return mysqlTable, version, err
		}
	}

	if characterSet == "UNKNOWN" || collation == "UNKNOWN" || characterSet == "" || collation == "" {
		characterSet, err = mysql.GetMySQLDBServerCharacterSet()
		if err != nil {
			return mysqlTable, version, err
		}
		collation, err = mysql.GetMySQLDBServerCollation()
		if err != nil {
			return mysqlTable, version, err
		}
	}

	comment, err := mysql.GetMySQLTableComment(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}
	mysqlTable.TableComment = strings.ToUpper(comment[0]["TABLE_COMMENT"])

	columns, err := getMySQLTableColumn(schemaName, tableName, mysql)
	if err != nil {
		return mysqlTable, version, err
	}

	indexes, err := getMySQLTableIndex(schemaName, tableName, mysql, targetDBType)
	if err != nil {
		return mysqlTable, version, err
	}

	puConstraints, fkConstraints, ckConstraints, err := getMySQLTableConstraint(schemaName, tableName, version, mysql, isTiDB)
	if err != nil {
		return mysqlTable, version, err
	}

	var parts []public.Partition

	isPart, err := mysql.IsMySQLPartitionTable(schemaName, tableName)
	if err != nil {
		return mysqlTable, version, err
	}
	if isPart {
		partInfo, err := mysql.GetMySQLPartitionTableINFO(schemaName, tableName)
		if err != nil {
			return mysqlTable, version, err
		}
		for _, part := range partInfo {
			parts = append(parts, public.Partition{
				PartitionKey:     strings.ToUpper(part["PARTITION_EXPRESS"]),
				PartitionType:    strings.ToUpper(part["PARTITIONING_TYPE"]),
				SubPartitionKey:  strings.ToUpper(part["SUBPARTITION_EXPRESS"]),
				SubPartitionType: strings.ToUpper(part["SUBPARTITIONING_TYPE"]),
			})
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

func getMySQLTableColumn(schemaName, tableName string, mysql *mysql.MySQL) (map[string]public.Column, error) {
	columnInfo, err := mysql.GetMySQLTableColumn(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	columns := make(map[string]public.Column, len(columnInfo))

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
		// 对比 oracle 会去掉前后一个单引号 ''PC'', mysql 增加前后单引号 ''PC''
		dataDefault = strings.TrimSpace(rowCol["DATA_DEFAULT"])

		if strings.HasPrefix(dataDefault, "'") {
			dataDefault = fmt.Sprintf("'%s", dataDefault)
		}

		if strings.HasSuffix(dataDefault, "'") {
			dataDefault = fmt.Sprintf("%s'", dataDefault)
		}

		columns[strings.ToUpper(rowCol["COLUMN_NAME"])] = public.Column{
			DataType: strings.ToUpper(rowCol["DATA_TYPE"]),
			ColumnInfo: public.ColumnInfo{
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

	return columns, nil
}

func getMySQLTableIndex(schemaName, tableName string, mysql *mysql.MySQL, targetDBType string) ([]public.Index, error) {
	var indexes []public.Index

	indexInfo, err := mysql.GetMySQLTableIndex(schemaName, tableName, targetDBType)
	if err != nil {
		return indexes, err
	}
	for _, indexCol := range indexInfo {
		indexes = append(indexes, public.Index{
			IndexInfo: public.IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexName: strings.ToUpper(indexCol["INDEX_NAME"]),
			IndexType: strings.ToUpper(indexCol["INDEX_TYPE"]),
			// MySQL 没有 domain 索引
			DomainIndexOwner: "",
			DomainIndexName:  "",
			DomainParameters: "",
		})
	}
	return indexes, nil
}

func getMySQLTableConstraint(schemaName, tableName, version string, mysql *mysql.MySQL, isTiDB bool) ([]public.ConstraintPUKey, []public.ConstraintForeign, []public.ConstraintCheck, error) {

	var (
		puConstraints []public.ConstraintPUKey
		fkConstraints []public.ConstraintForeign
		ckConstraints []public.ConstraintCheck
	)
	pkInfo, err := mysql.GetMySQLTablePrimaryKey(schemaName, tableName)
	if err != nil {
		return puConstraints, fkConstraints, ckConstraints, err
	}
	for _, pk := range pkInfo {
		puConstraints = append(puConstraints, public.ConstraintPUKey{
			ConstraintType:   strings.ToUpper(pk["CONSTRAINT_TYPE"]),
			ConstraintColumn: strings.ToUpper(pk["COLUMN_LIST"]),
		})
	}

	ukInfo, err := mysql.GetMySQLTableUniqueKey(schemaName, tableName)
	if err != nil {
		return puConstraints, fkConstraints, ckConstraints, err
	}
	for _, uk := range ukInfo {
		puConstraints = append(puConstraints, public.ConstraintPUKey{
			ConstraintType:   strings.ToUpper(uk["CONSTRAINT_TYPE"]),
			ConstraintColumn: strings.ToUpper(uk["COLUMN_LIST"]),
		})
	}

	if !isTiDB {
		fkInfo, err := mysql.GetMySQLTableForeignKey(schemaName, tableName)
		if err != nil {
			return puConstraints, fkConstraints, ckConstraints, err
		}
		for _, fk := range fkInfo {
			fkConstraints = append(fkConstraints, public.ConstraintForeign{
				ColumnName:            strings.ToUpper(fk["COLUMN_LIST"]),
				ReferencedTableSchema: strings.ToUpper(fk["R_OWNER"]),
				ReferencedTableName:   strings.ToUpper(fk["RTABLE_NAME"]),
				ReferencedColumnName:  strings.ToUpper(fk["RCOLUMN_LIST"]),
				DeleteRule:            strings.ToUpper(fk["DELETE_RULE"]),
				UpdateRule:            strings.ToUpper(fk["UPDATE_RULE"]),
			})
		}

		var dbVersion string
		if strings.Contains(version, common.MySQLVersionDelimiter) {
			dbVersion = strings.Split(version, common.MySQLVersionDelimiter)[0]
		} else {
			dbVersion = version
		}
		if common.VersionOrdinal(dbVersion) > common.VersionOrdinal(common.MySQLCheckConsVersion) {
			ckInfo, err := mysql.GetMySQLTableCheckKey(schemaName, tableName)
			if err != nil {
				return puConstraints, fkConstraints, ckConstraints, err
			}
			for _, ck := range ckInfo {
				ckConstraints = append(ckConstraints, public.ConstraintCheck{
					ConstraintExpression: strings.ToUpper(ck["SEARCH_CONDITION"]),
				})
			}
		}
	}
	return puConstraints, fkConstraints, ckConstraints, nil
}
