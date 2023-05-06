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
	"github.com/wentaojin/transferdb/common"
	"go.uber.org/zap"
	"strings"
)

func (m *MySQL) GetTiDBClusteredIndexValue() (string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, `select VARIABLE_VALUE from information_schema.session_variables where upper(variable_name) = upper('tidb_enable_clustered_index')`)
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", nil
	}
	return res[0]["VARIABLE_VALUE"], nil
}

func (m *MySQL) GetTiDBAlterPKValue() (string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, `select VARIABLE_VALUE from information_schema.session_variables where upper(variable_name) = upper('tidb_config')`)
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", nil
	}
	return res[0]["VARIABLE_VALUE"], nil
}

func (m *MySQL) IsExistMySQLSchema(schemaName string) (bool, error) {
	schemas, err := m.getMySQLSchema()
	if err != nil {
		return false, err
	}
	if !common.IsContainString(schemas, strings.ToUpper(schemaName)) {
		return false, nil
	}
	return true, nil
}

func (m *MySQL) FilterIntersectionMySQLTable(schemaName string, includeTables []string) ([]string, error) {
	tables, err := m.getMySQLTable(schemaName)
	if err != nil {
		return []string{}, err
	}
	var includeTbl []string
	for _, tbl := range includeTables {
		includeTbl = append(includeTbl, strings.ToUpper(tbl))
	}
	return common.FilterIntersectionStringItems(tables, includeTbl), nil
}

func (m *MySQL) RenameMySQLTableName(schemaName string, tableName string) error {
	backupTable := fmt.Sprintf("%s_bak", tableName)
	querySQL := fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`", schemaName, tableName, schemaName, backupTable)
	zap.L().Info("Exec SQL",
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.String("sql", fmt.Sprintf("%v", querySQL)))
	_, _, err := Query(m.Ctx, m.MySQLDB, querySQL)
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) IsExistMysqlIndex(schemaName, tableName, indexName string) bool {
	querySQL := fmt.Sprintf(`SELECT count(1) AS CT
FROM information_schema.statistics 
WHERE upper(table_schema) = upper('%s')
AND upper(table_name) = upper('%s')
AND upper(index_name) = upper('%s')`, schemaName, tableName, indexName)
	_, res, _ := Query(m.Ctx, m.MySQLDB, querySQL)
	if res[0]["CT"] == "0" {
		return false
	}
	return true
}

func (m *MySQL) getMySQLSchema() ([]string, error) {
	var (
		schemas []string
		err     error
	)
	cols, res, err := Query(m.Ctx, m.MySQLDB, `SELECT DISTINCT(schema_name) AS SCHEMA_NAME FROM information_schema.SCHEMATA`)
	if err != nil {
		return schemas, err
	}
	for _, col := range cols {
		for _, r := range res {
			schemas = append(schemas, strings.ToUpper(r[col]))
		}
	}
	return schemas, nil
}

func (m *MySQL) getMySQLTable(schemaName string) ([]string, error) {
	var (
		tables []string
		err    error
	)
	cols, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`select table_name from information_schema.tables where upper(table_schema) = upper('%s') and upper(table_type)=upper('base table')`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, col := range cols {
		for _, r := range res {
			tables = append(tables, strings.ToUpper(r[col]))
		}
	}
	return tables, nil
}

func (m *MySQL) GetMySQLNormalTable(schemaName string) ([]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where UPPER(TABLE_SCHEMA) = '%s' AND TABLE_TYPE = 'BASE TABLE'`, strings.ToUpper(schemaName)))
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

func (m *MySQL) GetMySQLPartitionTable(schemaName string) ([]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE UPPER(TABLE_SCHEMA) = '%s' AND PARTITION_NAME IS NOT NULL`, strings.ToUpper(schemaName)))
	if err != nil {
		return []string{}, err
	}
	var tables []string
	if len(res) > 0 {
		for _, r := range res {
			tables = append(tables, strings.ToUpper(r["TABLE_NAME"]))
		}
	}

	return tables, nil
}

func (m *MySQL) GetMySQLViewTable(schemaName string) ([]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT TABLE_SCHEMA,TABLE_NAME AS VIEW_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE UPPER(TABLE_SCHEMA) = '%s'`, strings.ToUpper(schemaName)))
	if err != nil {
		return []string{}, err
	}
	var tables []string
	if len(res) > 0 {
		for _, r := range res {
			tables = append(tables, r["VIEW_NAME"])
		}
	}

	return tables, nil
}

func (m *MySQL) GetMySQLPartitionTableDetailINFO(schemaName, tableName string) (string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SHOW CREATE TABLE %s.%s`, strings.ToUpper(schemaName), strings.ToUpper(tableName)))
	if err != nil {
		return "", err
	}

	partitonINFO := strings.Split(strings.ReplaceAll(res[0]["Create Table"], "`", ""), "PARTITION BY")[1]
	if err != nil {
		return partitonINFO, fmt.Errorf("get table paritiotn info failed: %v", res[0]["Create Table"])
	}
	return partitonINFO, nil
}

func (m *MySQL) GetMySQLTablePrimaryKey(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT
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
  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND UPPER(tc.TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(tc.TABLE_NAME) = UPPER('%s')
GROUP BY tc.CONSTRAINT_NAME,tc.CONSTRAINT_TYPE`, schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MySQL) GetMySQLTableUniqueKey(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT
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
  AND tc.CONSTRAINT_TYPE = 'UNIQUE'
  AND UPPER(tc.TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(tc.TABLE_NAME) = UPPER('%s')
GROUP BY tc.CONSTRAINT_NAME,tc.CONSTRAINT_TYPE`, schemaName, tableName))
	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MySQL) GetMySQLTableCheckKey(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT tc.CONSTRAINT_NAME,
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

func (m *MySQL) GetMySQLTableForeignKey(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT tc.CONSTRAINT_NAME,
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

func (m *MySQL) GetMySQLTableIndex(schemaName, tableName string, targetDBTye string) ([]map[string]string, error) {
	var query string
	mysqlVersion, err := m.GetMySQLDBVersion()
	if err != nil {
		return nil, err
	}

	if strings.EqualFold(targetDBTye, common.DatabaseTypeTiDB) {
		if strings.Contains(common.StringUPPER(mysqlVersion), common.DatabaseTypeTiDB) {
			query = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION,'') COLUMN_EXPRESSION,
		IF(NON_UNIQUE=1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMN_LIST
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
  INDEX_NAME NOT IN ('PRIMARY','UNIQUE')
  AND UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
GROUP BY INDEX_NAME,UNIQUENESS,INDEX_TYPE,COLUMN_EXPRESSION`, schemaName, tableName)
		} else {
			return nil, fmt.Errorf("target db type isn't tidb, please adjust target db type, currently target db version: [%v]", mysqlVersion)
		}
	} else {
		var mysqlDBVersion string

		if strings.Contains(mysqlVersion, common.MySQLVersionDelimiter) {
			mysqlDBVersion = strings.Split(mysqlVersion, common.MySQLVersionDelimiter)[0]
		} else {
			mysqlDBVersion = mysqlVersion
		}
		if common.VersionOrdinal(mysqlDBVersion) >= common.VersionOrdinal(common.MySQLExpressionIndexVersion) {
			query = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION,'') COLUMN_EXPRESSION,
		IF(NON_UNIQUE=1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMN_LIST
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
  INDEX_NAME NOT IN ('PRIMARY','UNIQUE')
  AND UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
GROUP BY INDEX_NAME,UNIQUENESS,INDEX_TYPE,COLUMN_EXPRESSION`, schemaName, tableName)
		} else {
			query = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IF(NON_UNIQUE=1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMN_LIST
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
  INDEX_NAME NOT IN ('PRIMARY','UNIQUE')
  AND UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
GROUP BY INDEX_NAME,UNIQUENESS,INDEX_TYPE`, schemaName, tableName)
		}
	}
	_, res, err := Query(m.Ctx, m.MySQLDB, query)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MySQL) GetMySQLTableNormalIndex(schemaName, tableName string, targetDBTye string) ([]map[string]string, error) {
	var query string
	mysqlVersion, err := m.GetMySQLDBVersion()
	if err != nil {
		return nil, err
	}

	if strings.EqualFold(targetDBTye, common.DatabaseTypeTiDB) {
		if strings.Contains(common.StringUPPER(mysqlVersion), common.DatabaseTypeTiDB) {
			query = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION,'') COLUMN_EXPRESSION,
		IF(NON_UNIQUE=1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMN_LIST
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
  INDEX_NAME NOT IN ('PRIMARY','UNIQUE')
  AND UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
  AND NON_UNIQUE = 1
GROUP BY INDEX_NAME,UNIQUENESS,INDEX_TYPE,COLUMN_EXPRESSION`, schemaName, tableName)
		} else {
			return nil, fmt.Errorf("target db type isn't tidb, please adjust target db type, currently target db version: [%v]", mysqlVersion)
		}
	} else {
		var mysqlDBVersion string

		if strings.Contains(mysqlVersion, common.MySQLVersionDelimiter) {
			mysqlDBVersion = strings.Split(mysqlVersion, common.MySQLVersionDelimiter)[0]
		} else {
			mysqlDBVersion = mysqlVersion
		}
		if common.VersionOrdinal(mysqlDBVersion) >= common.VersionOrdinal(common.MySQLExpressionIndexVersion) {
			query = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION,'') COLUMN_EXPRESSION,
		IF(NON_UNIQUE=1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMN_LIST
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
  INDEX_NAME NOT IN ('PRIMARY','UNIQUE')
  AND UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
  AND NON_UNIQUE = 1
GROUP BY INDEX_NAME,UNIQUENESS,INDEX_TYPE,COLUMN_EXPRESSION`, schemaName, tableName)
		} else {
			query = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IF(NON_UNIQUE=1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMN_LIST
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
  INDEX_NAME NOT IN ('PRIMARY','UNIQUE')
  AND UPPER(TABLE_SCHEMA) = UPPER('%s')
  AND UPPER(TABLE_NAME) = UPPER('%s')
  AND NON_UNIQUE = 1
GROUP BY INDEX_NAME,UNIQUENESS,INDEX_TYPE`, schemaName, tableName)
		}
	}
	_, res, err := Query(m.Ctx, m.MySQLDB, query)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MySQL) GetMySQLTableComment(schemaName, tableName string) ([]map[string]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT TABLE_NAME,TABLE_COMMENT
	FROM
	INFORMATION_SCHEMA.TABLES
	WHERE
	UPPER(table_schema) = UPPER('%s')
	AND UPPER(table_name)= UPPER('%s')`, schemaName, tableName))
	if err != nil {
		return res, err
	}

	return res, nil
}

func (m *MySQL) GetMySQLTableOriginDDL(schemaName, tableName string) (string, error) {
	showSQL := fmt.Sprintf(`SHOW CREATE TABLE %v.%v`, schemaName, tableName)
	_, res, err := Query(m.Ctx, m.MySQLDB, showSQL)
	if err != nil {
		return "", err
	}
	if len(res) > 1 {
		return "", fmt.Errorf("[%v] sql return result over one, current result [%v]", showSQL, len(res))
	}

	return res[0]["Create Table"], nil
}
