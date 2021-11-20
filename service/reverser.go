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
	"strings"

	"github.com/wentaojin/transferdb/utils"

	"go.uber.org/zap"
)

func (e *Engine) IsExistMySQLSchema(schemaName string) (bool, error) {
	schemas, err := e.getMySQLSchema()
	if err != nil {
		return false, err
	}
	if !utils.IsContainString(schemas, strings.ToUpper(schemaName)) {
		return false, nil
	}
	return true, nil
}

func (e *Engine) FilterIntersectionMySQLTable(schemaName string, includeTables []string) ([]string, error) {
	tables, err := e.getMySQLTable(schemaName)
	if err != nil {
		return []string{}, err
	}
	var includeTbl []string
	for _, tbl := range includeTables {
		includeTbl = append(includeTbl, strings.ToUpper(tbl))
	}
	return utils.FilterIntersectionStringItems(tables, includeTbl), nil
}

func (e *Engine) RenameMySQLTableName(schemaName string, tableName string) error {
	backupTable := fmt.Sprintf("%s_bak", tableName)
	querySQL := fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`", schemaName, tableName, schemaName, backupTable)
	Logger.Info("Exec SQL",
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.String("sql", fmt.Sprintf("%v", querySQL)))
	_, _, err := Query(e.MysqlDB, querySQL)
	if err != nil {
		return err
	}
	return nil
}

func (e *Engine) IsExistMysqlIndex(schemaName, tableName, indexName string) bool {
	querySQL := fmt.Sprintf(`SELECT count(1) AS CT
FROM information_schema.statistics 
WHERE upper(table_schema) = upper('%s')
AND upper(table_name) = upper('%s')
AND upper(index_name) = upper('%s')`, schemaName, tableName, indexName)
	_, res, _ := Query(e.MysqlDB, querySQL)
	if res[0]["CT"] == "0" {
		return false
	}
	return true
}

func (e *Engine) FilterDifferenceOracleTable(schemaName string, excludeTables []string) ([]string, error) {
	tables, err := e.GetOracleTable(schemaName)
	if err != nil {
		return []string{}, err
	}
	return utils.FilterDifferenceStringItems(tables, excludeTables), nil
}

func (e *Engine) GetOracleTableComment(schemaName string, tableName string) ([]map[string]string, error) {
	var (
		comments []map[string]string
		err      error
	)
	querySQL := fmt.Sprintf(`select table_name,table_type,comments 
from all_tab_comments 
where 
table_type = 'TABLE'
and upper(owner)=upper('%s')
and upper(table_name)=upper('%s')`, strings.ToUpper(schemaName), strings.ToUpper(tableName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return comments, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableColumn(schemaName string, tableName string) ([]map[string]string, error) {
	// all_tab_columns 视图 https://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2094.html
	querySQL := fmt.Sprintf(`select t.COLUMN_NAME,
	     t.DATA_TYPE,
		 t.CHAR_LENGTH,
		 NVL(t.CHAR_USED,'UNKNOWN') CHAR_USED,
	     NVL(t.DATA_LENGTH,0) AS DATA_LENGTH,
	     NVL(t.DATA_PRECISION,0) AS DATA_PRECISION,
	     NVL(t.DATA_SCALE,0) AS DATA_SCALE,
		decode(t.NULLABLE,'N','N','Y',(select decode(count(1),0,'Y','N') from dba_constraints con 
			 where con.owner=t.owner 
			 and con.table_name=t.table_name 
			 and replace(replace(upper(con.search_condition_vc),' ',''),'"','') like '%%'||upper(t.column_name)||'ISNOTNULL'||'%%')
			 ) NULLABLE,
	     t.DATA_DEFAULT,
	     c.COMMENTS
	from all_tab_columns t, all_col_comments c
	where t.table_name = c.table_name
	 and t.column_name = c.column_name
     and t.owner = c.owner
	 and upper(t.table_name) = upper('%s')
	 and upper(t.owner) = upper('%s')
    order by t.COLUMN_ID`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
	// for the primary key of an Engine table, you can use the following command to set whether the primary key takes effect.
	// disable the primary key: alter table tableName disable primary key;
	// enable the primary key: alter table tableName enable primary key;
	// primary key status Disabled will not do primary key processing
	querySQL := fmt.Sprintf(`select cu.constraint_name,
       LISTAGG(cu.column_name, ',') WITHIN GROUP(ORDER BY cu.POSITION) AS COLUMN_LIST
  from all_cons_columns cu, all_constraints au
 where cu.constraint_name = au.constraint_name
   and au.constraint_type = 'P'
   and au.STATUS = 'ENABLED'
   and upper(au.table_name) = upper('%s')
   and upper(cu.owner) = upper('%s')
 group by cu.constraint_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select cu.constraint_name,au.index_name,
       LISTAGG(cu.column_name, ',') WITHIN GROUP(ORDER BY cu.POSITION) AS column_list
  from all_cons_columns cu, all_constraints au
 where cu.constraint_name = au.constraint_name
   and cu.owner = au.owner
   and cu.table_name = au.table_name
   and au.constraint_type = 'U'
   and au.STATUS = 'ENABLED'
   and upper(au.table_name) = upper('%s')
   and upper(cu.owner) = upper('%s')
 group by cu.constraint_name,au.index_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableCheckKey(schemaName string, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select cu.constraint_name,SEARCH_CONDITION
          from all_cons_columns cu, all_constraints au
         where cu.constraint_name = au.constraint_name
           and au.constraint_type = 'C'
           and au.STATUS = 'ENABLED'
           and upper(au.table_name) = upper('%s')
           and upper(cu.owner) = upper('%s')`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
	)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableForeignKey(schemaName string, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`with temp1 as
 (select
         t1.OWNER,
         t1.r_owner,
         t1.constraint_name,
         t1.r_constraint_name,
         t1.DELETE_RULE,
         LISTAGG(a1.column_name, ',') WITHIN GROUP(ORDER BY a1.POSITION) AS COLUMN_LIST
    from all_constraints t1, all_cons_columns a1
   where t1.constraint_name = a1.constraint_name
     AND upper(t1.table_name) = upper('%s')
     AND upper(t1.owner) = upper('%s')
     AND t1.STATUS = 'ENABLED'
     AND t1.Constraint_Type = 'R'
   group by t1.OWNER, t1.r_owner, t1.constraint_name, t1.r_constraint_name,t1.DELETE_RULE),
temp2 as
 (select t1.owner,
         t1.TABLE_NAME,
         t1.constraint_name,
         LISTAGG(a1.column_name, ',') WITHIN GROUP(ORDER BY a1.POSITION) AS COLUMN_LIST
    from all_constraints t1, all_cons_columns a1
   where t1.constraint_name = a1.constraint_name
     AND upper(t1.owner) = upper('%s')
     AND t1.STATUS = 'ENABLED'
     AND t1.Constraint_Type = 'P'
   group by t1.owner,t1.TABLE_NAME, t1.r_owner, t1.constraint_name)
select x.constraint_name,
       x.COLUMN_LIST,
       x.r_owner,
       y.TABLE_NAME as RTABLE_NAME,
       y.COLUMN_LIST as RCOLUMN_LIST,
       x.DELETE_RULE
  from temp1 x, temp2 y
 where x.r_owner = y.owner
   and x.r_constraint_name = y.constraint_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
		strings.ToUpper(schemaName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	temp.TABLE_NAME,
	temp.UNIQUENESS,--是否唯一索引
	temp.INDEX_NAME,
	temp.INDEX_TYPE,
	temp.column_list,
	E.COLUMN_EXPRESSION 
FROM
	(
SELECT
	I.TABLE_OWNER,
	I.TABLE_NAME,
	I.UNIQUENESS,--是否唯一索引
	I.INDEX_NAME,
	I.INDEX_TYPE,
	LISTAGG ( T.COLUMN_NAME, ',' ) WITHIN GROUP ( ORDER BY T.COLUMN_POSITION ) AS COLUMN_LIST 
FROM
	ALL_INDEXES I,
	ALL_IND_COLUMNS T 
WHERE
	I.INDEX_NAME = T.INDEX_NAME 
	AND I.TABLE_OWNER = T.TABLE_OWNER 
	AND I.TABLE_NAME = T.TABLE_NAME 
	AND I.UNIQUENESS = 'UNIQUE' 
	AND UPPER( I.TABLE_NAME ) = upper( '%s' ) 
	AND UPPER( I.TABLE_OWNER ) = upper( '%s' ) 
	-- 排除主键、唯一约束索引
	AND NOT EXISTS (
	SELECT
		1 
	FROM
		ALL_CONSTRAINTS C 
	WHERE
		I.INDEX_NAME = C.INDEX_NAME 
		AND I.TABLE_OWNER = C.OWNER 
		AND I.TABLE_NAME = C.TABLE_NAME 
		AND C.CONSTRAINT_TYPE IN ('P','U')
	) 
GROUP BY
	I.TABLE_OWNER,
	I.TABLE_NAME,
	I.UNIQUENESS,--是否唯一索引
	I.INDEX_NAME,
	I.INDEX_TYPE
) temp
	LEFT JOIN ALL_IND_EXPRESSIONS E ON temp.TABLE_NAME = E.TABLE_NAME 
AND temp.TABLE_OWNER = E.TABLE_OWNER 
	AND temp.INDEX_NAME = E.INDEX_NAME`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	temp.TABLE_NAME,
	temp.UNIQUENESS,--是否唯一索引
	temp.INDEX_NAME,
	temp.INDEX_TYPE,
	temp.column_list,
	E.COLUMN_EXPRESSION 
FROM
	(
	SELECT
		T.TABLE_OWNER,
		T.TABLE_NAME,
		I.UNIQUENESS,--是否唯一索引
		T.INDEX_NAME,
		I.INDEX_TYPE,
		LISTAGG ( T.COLUMN_NAME, ',' ) WITHIN GROUP ( ORDER BY T.COLUMN_POSITION ) AS column_list 
	FROM
		ALL_IND_COLUMNS T,
		ALL_INDEXES I 
	WHERE t.table_owner=i.table_owner 
		and t.table_name=i.table_name
		and t.index_name=i.index_name
		and i.uniqueness='NONUNIQUE'
		-- AND I.INDEX_TYPE != 'FUNCTION-BASED NORMAL' --排除基于函数的索引
		-- AND I.INDEX_TYPE != 'BITMAP' --排除位图索引
	  AND T.TABLE_NAME = upper( '%s' ) 
		AND T.TABLE_OWNER = upper( '%s' ) 
		and not exists (
		select 1 from ALL_CONSTRAINTS C where 
		c.owner=i.table_owner 
		and c.table_name=i.table_name
		and c.index_name=i.index_name)
	GROUP BY
		T.TABLE_OWNER,
		T.TABLE_NAME,
		I.UNIQUENESS,--是否唯一索引
		T.INDEX_NAME,
		I.INDEX_TYPE 
	) temp
	LEFT JOIN ALL_IND_EXPRESSIONS E ON temp.TABLE_NAME = E.TABLE_NAME 
AND temp.TABLE_OWNER = E.TABLE_OWNER 
	AND temp.INDEX_NAME = E.INDEX_NAME`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) getOracleSchema() ([]string, error) {
	var (
		schemas []string
		err     error
	)
	cols, res, err := Query(e.OracleDB, `SELECT DISTINCT username FROM ALL_USERS`)
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

func (e *Engine) GetOracleTable(schemaName string) ([]string, error) {
	var (
		tables []string
		err    error
	)
	cols, res, err := Query(e.OracleDB, fmt.Sprintf(`SELECT table_name FROM ALL_TABLES WHERE UPPER(owner) = UPPER('%s')`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, col := range cols {
		for _, r := range res {
			tables = append(tables, r[col])
		}
	}
	return tables, nil
}

func (e *Engine) FilterOraclePartitionTable(schemaName string, tableSlice []string) ([]string, error) {
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`select table_name AS TABLE_NAME
  from all_tables
 where partitioned = 'YES'
   and upper(owner) = upper('%s')`, schemaName))
	if err != nil {
		return []string{}, err
	}

	var tables []string
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return utils.FilterIntersectionStringItems(tableSlice, tables), nil
}

func (e *Engine) getMySQLSchema() ([]string, error) {
	var (
		schemas []string
		err     error
	)
	cols, res, err := Query(e.MysqlDB, `SELECT DISTINCT(schema_name) AS SCHEMA_NAME FROM information_schema.SCHEMATA`)
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

func (e *Engine) getMySQLTable(schemaName string) ([]string, error) {
	var (
		tables []string
		err    error
	)
	cols, res, err := Query(e.MysqlDB, fmt.Sprintf(`select table_name from information_schema.tables where upper(table_schema) = upper('%s') and upper(table_type)=upper('base table')`, schemaName))
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
