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
package service

import (
	"fmt"
)

/*
	Oracle
*/
func (e *Engine) GetOracleDBCharacterSet() (string, error) {
	querySQL := fmt.Sprintf(`select userenv('language') AS LANG from dual`)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res[0]["LANG"], err
	}
	return res[0]["LANG"], nil
}

func (e *Engine) IsOraclePartitionTable(schemaName, tableName string) (bool, error) {
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`select count(1) AS COUNT
  from all_tables
 where partitioned = 'YES'
   and upper(owner) = upper('%s')
   and upper(table_name) = upper('%s')`, schemaName, tableName))
	if err != nil {
		return false, err
	}
	if res[0]["COUNT"] == "0" {
		return false, nil
	}
	return true, nil
}

func (e *Engine) GetOraclePartitionTableINFO(schemaName, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT L.PARTITIONING_TYPE,
       L.SUBPARTITIONING_TYPE,
       L.PARTITION_EXPRESS,
       LISTAGG(skc.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY skc.COLUMN_POSITION) AS SUBPARTITION_EXPRESS
FROM (SELECT pt.OWNER,
             pt.TABLE_NAME,
             pt.PARTITIONING_TYPE,
             pt.SUBPARTITIONING_TYPE,
             LISTAGG(ptc.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY ptc.COLUMN_POSITION) AS PARTITION_EXPRESS
      FROM ALL_PART_TABLES pt,
           ALL_PART_KEY_COLUMNS ptc
      WHERE pt.OWNER = ptc.OWNER
        AND pt.TABLE_NAME = ptc.NAME
        AND ptc.OBJECT_TYPE = 'TABLE'
        AND UPPER(pt.OWNER) = UPPER('%s')
        AND UPPER(pt.TABLE_NAME) = UPPER('%s')
      GROUP BY pt.OWNER, pt.TABLE_NAME, pt.PARTITIONING_TYPE,
               pt.SUBPARTITIONING_TYPE) L,
     ALL_SUBPART_KEY_COLUMNS skc
WHERE L.OWNER = skc.OWNER
  AND L.TABLE_NAME = skc.NAME
GROUP BY  L.PARTITIONING_TYPE,
       L.SUBPARTITIONING_TYPE,
       L.PARTITION_EXPRESS`, schemaName, tableName)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

/*
	MySQL
*/
func (e *Engine) GetMySQLDBServerCharacterSet() (string, error) {
	_, res, err := Query(e.MysqlDB, `SHOW VARIABLES LIKE 'character_set_server'`)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (e *Engine) GetMySQLDBServerCollation() (string, error) {
	_, res, err := Query(e.MysqlDB, `SHOW VARIABLES LIKE 'collation_server'`)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (e *Engine) GetMySQLDBVersion() (string, error) {
	_, res, err := Query(e.MysqlDB, `select version() AS VERSION`)
	if err != nil {
		return "", err
	}
	return res[0]["VERSION"], nil
}

func (e *Engine) GetMySQLTableComment(schemaName, tableName string) (string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT TABLE_NAME,TABLE_COMMENT
	FROM
	INFORMATION_SCHEMA.TABLES
	WHERE
	UPPER(table_schema) = UPPER('%s')
	AND UPPER(table_name)= UPPER('%s')`, schemaName, tableName))
	if err != nil {
		return "", err
	}
	return res[0]["TABLE_COMMENT"], nil
}

func (e *Engine) GetMySQLTableColumn(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT COLUMN_NAME,
       DATA_TYPE,
       IFNULL(CHARACTER_MAXIMUM_LENGTH,0) DATA_LENGTH,
       IFNULL(NUMERIC_SCALE,0) DATA_SCALE,
       IFNULL(NUMERIC_PRECISION,0) DATA_PRECISION,
       IF(IS_NULLABLE = 'NO', 'N', 'Y') NULLABLE,
       COLUMN_DEFAULT DATA_DEFAULT,
       COLUMN_COMMENT COMMENTS,
       CHARACTER_SET_NAME,
       COLLATION_NAME
FROM information_schema.COLUMNS
WHERE UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
ORDER BY ORDINAL_POSITION`, schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetMySQLTableIndex(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT INDEX_NAME,IF(NON_UNIQUE=1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMN_LIST
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
  INDEX_NAME NOT IN ('PRIMARY','UNIQUE')
  AND UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
GROUP BY INDEX_NAME,UNIQUENESS`, schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetMySQLTablePrimaryAndUniqueKey(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT
       tc.CONSTRAINT_NAME,
	CASE
		tc.CONSTRAINT_TYPE 
		WHEN 'PRIMARY KEY' THEN
		'PK' 
		WHEN 'UNIQUE' THEN
		'UK' 
		ELSE 'UKNOWN'
	END AS CONSTRAINT_TYPE,
       GROUP_CONCAT(ku.COLUMN_NAME ORDER BY ku.ORDINAL_POSITION SEPARATOR ',') COLUMN_LIST
FROM information_schema.TABLE_CONSTRAINTS tc,
     information_schema.KEY_COLUMN_USAGE ku
WHERE tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
  AND tc.TABLE_NAME = ku.TABLE_NAME
 AND tc.CONSTRAINT_SCHEMA = ku.CONSTRAINT_SCHEMA
  AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
  AND tc.CONSTRAINT_CATALOG = ku.CONSTRAINT_CATALOG
  AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY','UNIQUE')
  AND UPPER(tc.TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(tc.TABLE_NAME) = UPPER('%s')
GROUP BY tc.CONSTRAINT_NAME,tc.CONSTRAINT_TYPE`, schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetMySQLTableForeignKey(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT tc.CONSTRAINT_NAME,
		ku.COLUMN_NAME COLUMN_LIST,
		ku.REFERENCED_TABLE_SCHEMA R_OWNER,
		ku.REFERENCED_TABLE_NAME RTABLE_NAME,
		ku.REFERENCED_COLUMN_NAME RCOLUMN_LIST,
		rc.DELETE_RULE,
		rc.UPDATE_RULE
	FROM information_schema.TABLE_CONSTRAINTS tc,
		information_schema.KEY_COLUMN_USAGE ku,
		INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
	WHERE tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
	AND tc.TABLE_NAME = ku.TABLE_NAME
	AND tc.CONSTRAINT_SCHEMA = ku.CONSTRAINT_SCHEMA
	AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
	AND tc.CONSTRAINT_CATALOG = ku.CONSTRAINT_CATALOG
  	AND tc.CONSTRAINT_CATALOG=rc.CONSTRAINT_CATALOG
	AND tc.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
	AND tc.TABLE_NAME = rc.TABLE_NAME
	AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
	AND UPPER(tc.TABLE_SCHEMA) = UPPER('%s')
	AND UPPER(tc.TABLE_NAME) = UPPER('%s')`, schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetMySQLTableCheckKey(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT tc.CONSTRAINT_NAME,
       rc.CHECK_CLAUSE SEARCH_CONDITION
FROM information_schema.TABLE_CONSTRAINTS tc,
     INFORMATION_SCHEMA.CHECK_CONSTRAINTS rc
WHERE tc.CONSTRAINT_CATALOG = rc.CONSTRAINT_CATALOG
  AND tc.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
  AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
  AND tc.CONSTRAINT_TYPE = 'CHECK'
  AND UPPER(tc.TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(tc.TABLE_NAME) = UPPER('%s')`, schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) IsMySQLPartitionTable(schemaName, tableName string) (bool, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT IFNULL(PARTITION_NAME,0) PARTITION_NAME
FROM information_schema.PARTITIONS
WHERE UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
LIMIT 1`, schemaName, tableName))
	if err != nil {
		return false, err
	}
	if res[0]["PARTITION_NAME"] == "0" {
		return false, nil
	}
	return true, nil
}

func (e *Engine) GetMySQLPartitionTableINFO(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf("SELECT TRIM('`' FROM PARTITION_EXPRESSION) PARTITION_EXPRESSION,PARTITION_METHOD,TRIM('`' FROM SUBPARTITION_EXPRESSION) SUBPARTITION_EXPRESSION,SUBPARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS WHERE UPPER(TABLE_SCHEMA) = UPPER('%s') AND UPPER(TABLE_NAME) = UPPER('%s') LIMIT 1", schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}
