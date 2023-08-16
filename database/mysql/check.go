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
package mysql

import (
	"fmt"
	"strings"
)

func (m *MySQL) GetMySQLDBVersion() (string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, `select version() AS VERSION`)
	if err != nil {
		return "", err
	}
	return res[0]["VERSION"], nil
}

func (m *MySQL) GetMySQLTableCharacterSetAndCollation(schemaName, tableName string) (string, string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT
	IFNULL(CCSA.CHARACTER_SET_NAME,'UNKNOWN') CHARACTER_SET_NAME,
	IFNULL(T.table_collation,'UNKNOWN') COLLATION
FROM
	information_schema.TABLES T,
	information_schema.COLLATION_CHARACTER_SET_APPLICABILITY CCSA 
WHERE
	CCSA.collation_name = T.table_collation 
	AND UPPER( T.table_schema ) = UPPER( '%s' ) 
	AND UPPER( T.table_name ) = UPPER( '%s' )`, schemaName, tableName))
	if err != nil {
		return "", "", err
	}
	return res[0]["CHARACTER_SET_NAME"], res[0]["COLLATION"], nil
}

func (m *MySQL) GetMySQLTableColumn(schemaName, tableName string) ([]map[string]string, error) {
	var (
		res []map[string]string
		err error
	)

	_, res, err = Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT COLUMN_NAME,
		DATA_TYPE,
		IFNULL(CHARACTER_MAXIMUM_LENGTH,0) DATA_LENGTH,
		IFNULL(NUMERIC_SCALE,0) DATA_SCALE,
		IFNULL(NUMERIC_PRECISION,0) DATA_PRECISION,
		IFNULL(DATETIME_PRECISION,0) DATETIME_PRECISION,
		IF(IS_NULLABLE = 'NO', 'N', 'Y') NULLABLE,
		IFNULL(COLUMN_DEFAULT,'NULLSTRING') DATA_DEFAULT,
		IFNULL(COLUMN_COMMENT,'') COMMENTS,
		IFNULL(CHARACTER_SET_NAME,'UNKNOWN') CHARACTER_SET_NAME,
		IFNULL(COLLATION_NAME,'UNKNOWN') COLLATION_NAME
 FROM information_schema.COLUMNS
 WHERE UPPER(TABLE_SCHEMA) = UPPER('%s')
   AND UPPER(TABLE_NAME) = UPPER('%s')
 ORDER BY ORDINAL_POSITION`, schemaName, tableName))

	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MySQL) GetMySQLTableColumnComment(schemaName, tableName string) ([]map[string]string, error) {
	var (
		res []map[string]string
		err error
	)

	_, res, err = Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT COLUMN_NAME,
		IFNULL(COLUMN_COMMENT,'') COMMENTS
 FROM information_schema.COLUMNS
 WHERE UPPER(TABLE_SCHEMA) = UPPER('%s')
   AND UPPER(TABLE_NAME) = UPPER('%s')
 ORDER BY ORDINAL_POSITION`, schemaName, tableName))

	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MySQL) GetMySQLDBServerCharacterSet() (string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, `SHOW VARIABLES LIKE 'character_set_server'`)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (m *MySQL) GetMySQLDBServerCollation() (string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, `SHOW VARIABLES LIKE 'collation_server'`)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (m *MySQL) IsExistMySQLTableCharacterSetAndCollation(schemaName, tableName string) (bool, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT
	COUNT(1) COUNT
FROM
	information_schema.TABLES T,
	information_schema.COLLATION_CHARACTER_SET_APPLICABILITY CCSA 
WHERE
	CCSA.collation_name = T.table_collation 
	AND UPPER( T.table_schema ) = UPPER( '%s' ) 
	AND UPPER( T.table_name ) = UPPER( '%s' )`, schemaName, tableName))
	if err != nil {
		return false, err
	}
	if res[0]["COUNT"] == "0" {
		return false, nil
	}
	return true, nil
}

func (m *MySQL) IsMySQLPartitionTable(schemaName, tableName string) (bool, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT IFNULL(PARTITION_NAME,0) PARTITION_NAME
FROM information_schema.PARTITIONS
WHERE UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
LIMIT 1`, schemaName, tableName))
	if err != nil {
		return false, err
	}
	if len(res) == 0 || res[0]["PARTITION_NAME"] == "0" {
		return false, nil
	}

	return true, nil
}

func (m *MySQL) GetMySQLPartitionTableINFO(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf("SELECT TRIM('`' FROM PARTITION_EXPRESSION) PARTITION_EXPRESSION,PARTITION_METHOD,TRIM('`' FROM SUBPARTITION_EXPRESSION) SUBPARTITION_EXPRESSION,SUBPARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS WHERE UPPER(TABLE_SCHEMA) = UPPER('%s') AND UPPER(TABLE_NAME) = UPPER('%s') LIMIT 1", schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MySQL) GetMySQLTable(schemaName string) ([]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where UPPER(TABLE_SCHEMA) = '%s'`, strings.ToUpper(schemaName)))
	if err != nil {
		return []string{}, err
	}
	var tables []string
	if len(res) > 0 {
		for _, r := range res {
			tables = append(tables, r["TABLE_NAME"])
		}
	}

	return tables, nil
}
