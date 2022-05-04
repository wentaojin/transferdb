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
	"regexp"
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
from dba_tab_comments 
where 
table_type = 'TABLE'
and upper(owner)=upper('%s')
and upper(table_name)=upper('%s')`, strings.ToUpper(schemaName), strings.ToUpper(tableName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return comments, err
	}
	if len(res) == 0 {
		return res, fmt.Errorf("oracle table [%s.%s] comment can't be null，result: [%v]", schemaName, tableName, res)
	}
	if len(res) > 1 {
		return res, fmt.Errorf("oracle schema [%s] table [%s] comments exist multiple values: [%v]", schemaName, tableName, res)
	}

	return res, nil
}

func (e *Engine) GetOracleTableColumn(schemaName string, tableName string, oraCollation bool) ([]map[string]string, error) {
	var querySQL string

	if oraCollation {
		querySQL = fmt.Sprintf(`select t.COLUMN_NAME,
	    t.DATA_TYPE,
		 t.CHAR_LENGTH,
		 NVL(t.CHAR_USED,'UNKNOWN') CHAR_USED,
	    NVL(t.DATA_LENGTH,0) AS DATA_LENGTH,
	    NVL(t.DATA_PRECISION,0) AS DATA_PRECISION,
	    NVL(t.DATA_SCALE,0) AS DATA_SCALE,
		t.NULLABLE,
	    t.DATA_DEFAULT,
		DECODE(t.COLLATION,'USING_NLS_COMP',(SELECT VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP'),t.COLLATION) COLLATION,
	    c.COMMENTS
	from dba_tab_columns t, dba_col_comments c
	where t.table_name = c.table_name
	and t.column_name = c.column_name
	and t.owner = c.owner
	and upper(t.owner) = upper('%s')
	and upper(t.table_name) = upper('%s')
	order by t.COLUMN_ID`,
			strings.ToUpper(schemaName),
			strings.ToUpper(tableName))
	} else {
		querySQL = fmt.Sprintf(`select t.COLUMN_NAME,
	    t.DATA_TYPE,
		 t.CHAR_LENGTH,
		 NVL(t.CHAR_USED,'UNKNOWN') CHAR_USED,
	    NVL(t.DATA_LENGTH,0) AS DATA_LENGTH,
	    NVL(t.DATA_PRECISION,0) AS DATA_PRECISION,
	    NVL(t.DATA_SCALE,0) AS DATA_SCALE,
		t.NULLABLE,
	    t.DATA_DEFAULT,
	    c.COMMENTS
	from dba_tab_columns t, dba_col_comments c
	where t.table_name = c.table_name
	and t.column_name = c.column_name
	and t.owner = c.owner
	and upper(t.owner) = upper('%s')
	and upper(t.table_name) = upper('%s')
	order by t.COLUMN_ID`,
			strings.ToUpper(schemaName),
			strings.ToUpper(tableName))
	}

	_, queryRes, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return queryRes, err
	}
	if len(queryRes) == 0 {
		return queryRes, fmt.Errorf("oracle table [%s.%s] column info cann't be null", schemaName, tableName)
	}

	// check constraints notnull
	// search_condition long datatype
	_, condRes, err := Query(e.OracleDB, fmt.Sprintf(`SELECT
				col.COLUMN_NAME,
				cons.SEARCH_CONDITION
				FROM
				DBA_CONS_COLUMNS col,
				DBA_CONSTRAINTS cons
				WHERE
				col.OWNER = cons.OWNER
				AND col.TABLE_NAME = cons.TABLE_NAME
				AND col.CONSTRAINT_NAME = cons.CONSTRAINT_NAME
				AND cons.CONSTRAINT_TYPE = 'C'
				AND upper(col.OWNER) = '%s'
				AND upper(col.TABLE_NAME) = '%s'`, strings.ToUpper(schemaName), strings.ToUpper(tableName)))
	if err != nil {
		return queryRes, err
	}

	if len(condRes) == 0 {
		return queryRes, nil
	}

	rep, err := regexp.Compile(`(^.*)(?i:IS NOT NULL)`)
	if err != nil {
		return queryRes, fmt.Errorf("check notnull constraint regexp complile failed: %v", err)
	}
	for _, r := range queryRes {
		for _, c := range condRes {
			if r["COLUMN_NAME"] == c["COLUMN_NAME"] && r["NULLABLE"] == "Y" {
				// 检查约束非空检查
				if rep.MatchString(c["SEARCH_CONDITION"]) {
					r["NULLABLE"] = "N"
				}
			}
		}
	}
	return queryRes, nil
}

// ORACLE XML 限制
// func (e *Engine) GetOracleTableColumn(schemaName string, tableName string, oraCollation bool) ([]map[string]string, error) {
//	var querySQL string
//	if oraCollation {
//		querySQL = fmt.Sprintf(`select t.COLUMN_NAME,
//	    t.DATA_TYPE,
//		 t.CHAR_LENGTH,
//		 NVL(t.CHAR_USED,'UNKNOWN') CHAR_USED,
//	    NVL(t.DATA_LENGTH,0) AS DATA_LENGTH,
//	    NVL(t.DATA_PRECISION,0) AS DATA_PRECISION,
//	    NVL(t.DATA_SCALE,0) AS DATA_SCALE,
//		DECODE(t.NULLABLE,'N','N','Y',(SELECT
//	DECODE( COUNT( 1 ), 0, 'Y', 'N' )
//FROM
//	XMLTABLE (
//		'/ROWSET/ROW' PASSING ( SELECT DBMS_XMLGEN.GETXMLTYPE (
//				q'[SELECT
//				col.COLUMN_NAME,
//				cons.search_condition
//				FROM
//				DBA_CONS_COLUMNS col,
//				DBA_CONSTRAINTS cons
//				WHERE
//				col.OWNER = cons.OWNER
//				AND col.TABLE_NAME = cons.TABLE_NAME
//				AND col.CONSTRAINT_NAME = cons.CONSTRAINT_NAME
//				AND cons.CONSTRAINT_TYPE = 'C'
//				AND upper(col.OWNER) = '%s'
//				AND upper(col.TABLE_NAME) = '%s']' ) FROM DUAL ) COLUMNS column_name VARCHAR2 ( 30 ) PATH 'COLUMN_NAME',
//		search_condition VARCHAR2 ( 4000 )
//	) xs
//WHERE
//	xs.COLUMN_NAME = t.COLUMN_NAME AND
//	REPLACE (
//		REPLACE ( upper( xs.search_condition ), ' ', '' ),'"',	'' 	) LIKE '%%' || upper( t.column_name ) || 'ISNOTNULL' || '%%')
//			 ) NULLABLE,
//	    t.DATA_DEFAULT,
//		DECODE(t.COLLATION,'USING_NLS_COMP',(SELECT VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP'),t.COLLATION) COLLATION,
//	    c.COMMENTS
//	from dba_tab_columns t, dba_col_comments c
//	where t.table_name = c.table_name
//	and t.column_name = c.column_name
//	and t.owner = c.owner
//	and upper(t.owner) = upper('%s')
//	and upper(t.table_name) = upper('%s')
//	order by t.COLUMN_ID`,
//			strings.ToUpper(schemaName),
//			strings.ToUpper(tableName),
//			strings.ToUpper(schemaName),
//			strings.ToUpper(tableName))
//	} else {
//		querySQL = fmt.Sprintf(`select t.COLUMN_NAME,
//	    t.DATA_TYPE,
//		 t.CHAR_LENGTH,
//		 NVL(t.CHAR_USED,'UNKNOWN') CHAR_USED,
//	    NVL(t.DATA_LENGTH,0) AS DATA_LENGTH,
//	    NVL(t.DATA_PRECISION,0) AS DATA_PRECISION,
//	    NVL(t.DATA_SCALE,0) AS DATA_SCALE,
//		DECODE(t.NULLABLE,'N','N','Y',(SELECT
//	DECODE( COUNT( 1 ), 0, 'Y', 'N' )
//FROM
//	XMLTABLE (
//		'/ROWSET/ROW' PASSING ( SELECT DBMS_XMLGEN.GETXMLTYPE (
//				q'[SELECT
//				col.COLUMN_NAME,
//				cons.search_condition
//				FROM
//				DBA_CONS_COLUMNS col,
//				DBA_CONSTRAINTS cons
//				WHERE
//				col.OWNER = cons.OWNER
//				AND col.TABLE_NAME = cons.TABLE_NAME
//				AND col.CONSTRAINT_NAME = cons.CONSTRAINT_NAME
//				AND cons.CONSTRAINT_TYPE = 'C'
//				AND upper(col.OWNER) = '%s'
//				AND upper(col.TABLE_NAME) = '%s']' ) FROM DUAL ) COLUMNS column_name VARCHAR2 ( 30 ) PATH 'COLUMN_NAME',
//		search_condition VARCHAR2 ( 4000 )
//	) xs
//WHERE
//	xs.COLUMN_NAME = t.COLUMN_NAME AND
//	REPLACE (
//		REPLACE ( upper( xs.search_condition ), ' ', '' ),'"',	'' 	) LIKE '%%' || upper( t.column_name ) || 'ISNOTNULL' || '%%')
//			 ) NULLABLE,
//	    t.DATA_DEFAULT,
//	    c.COMMENTS
//	from dba_tab_columns t, dba_col_comments c
//	where t.table_name = c.table_name
//	and t.column_name = c.column_name
//	and t.owner = c.owner
//	and upper(t.owner) = upper('%s')
//	and upper(t.table_name) = upper('%s')
//	order by t.COLUMN_ID`,
//			strings.ToUpper(schemaName),
//			strings.ToUpper(tableName),
//			strings.ToUpper(schemaName),
//			strings.ToUpper(tableName))
//	}
//
//	_, res, err := Query(e.OracleDB, querySQL)
//	if err != nil {
//		return res, err
//	}
//	if len(res) == 0 {
//		return res, fmt.Errorf("oracle table [%s.%s] column info cann't be null", schemaName, tableName)
//	}
//	return res, nil
//}

func (e *Engine) GetOracleTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
	// for the primary key of an Engine table, you can use the following command to set whether the primary key takes effect.
	// disable the primary key: alter table tableName disable primary key;
	// enable the primary key: alter table tableName enable primary key;
	// primary key status Disabled will not do primary key processing
	querySQL := fmt.Sprintf(`select cu.constraint_name,
       LISTAGG(cu.column_name, ',') WITHIN GROUP(ORDER BY cu.POSITION) AS COLUMN_LIST
  from dba_cons_columns cu, dba_constraints au
 where cu.constraint_name = au.constraint_name
   and au.constraint_type = 'P'
   and au.STATUS = 'ENABLED'
   and cu.owner = au.owner
   and cu.table_name = au.table_name
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
  from dba_cons_columns cu, dba_constraints au
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
          from dba_cons_columns cu, dba_constraints au
         where cu.owner=au.owner
           and cu.table_name=au.table_name
           and cu.constraint_name = au.constraint_name
           and au.constraint_type = 'C'
           and au.STATUS = 'ENABLED'
           and upper(au.table_name) = upper('%s')
           and upper(au.owner) = upper('%s')`,
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
    from dba_constraints t1, dba_cons_columns a1
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
    from dba_constraints t1, dba_cons_columns a1
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
	temp.UNIQUENESS,
	temp.INDEX_NAME,
	temp.INDEX_TYPE,
	temp.ITYP_OWNER,
	temp.ITYP_NAME,
	temp.PARAMETERS,
	LISTAGG ( temp.COLUMN_NAME, ',' ) WITHIN GROUP ( ORDER BY temp.COLUMN_POSITION ) AS COLUMN_LIST 
FROM
	(
SELECT
	I.TABLE_OWNER,
	I.TABLE_NAME,
	I.UNIQUENESS,
	I.INDEX_NAME,
	I.INDEX_TYPE,
	NVL(I.ITYP_OWNER,'') ITYP_OWNER,
	NVL(I.ITYP_NAME,'') ITYP_NAME,
	NVL(I.PARAMETERS,'') PARAMETERS,
	DECODE((SELECT
	COUNT( 1 ) 
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = T.TABLE_OWNER
	AND S.TABLE_NAME = T.TABLE_NAME
	AND S.INDEX_OWNER = T.INDEX_OWNER
	AND S.INDEX_NAME = T.INDEX_NAME
	AND S.COLUMN_POSITION = T.COLUMN_POSITION),0,T.COLUMN_NAME, (SELECT
	xs.COLUMN_EXPRESSION
FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING ( SELECT DBMS_XMLGEN.GETXMLTYPE ( 
				q'[SELECT
	S.INDEX_OWNER,
	S.INDEX_NAME,
	S.COLUMN_EXPRESSION,
	S.COLUMN_POSITION
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = '%s'
	AND S.TABLE_NAME = '%s']' ) FROM DUAL ) COLUMNS 
	INDEX_OWNER VARCHAR2 ( 30 ) PATH 'INDEX_OWNER',
	INDEX_NAME VARCHAR2 ( 30 ) PATH 'INDEX_NAME',
	COLUMN_POSITION VARCHAR2 ( 30 ) PATH 'COLUMN_POSITION',
	COLUMN_EXPRESSION VARCHAR2 ( 4000 ) 
	) xs 
WHERE
xs.INDEX_OWNER = T.INDEX_OWNER
	AND xs.INDEX_NAME = T.INDEX_NAME
AND xs.COLUMN_POSITION = T.COLUMN_POSITION)) COLUMN_NAME,
		T.COLUMN_POSITION
FROM
	DBA_INDEXES I,
	DBA_IND_COLUMNS T 
WHERE
	I.INDEX_NAME = T.INDEX_NAME 
	AND I.TABLE_OWNER = T.TABLE_OWNER 
	AND I.TABLE_NAME = T.TABLE_NAME 
	AND I.UNIQUENESS = 'UNIQUE'
	AND I.TABLE_OWNER = '%s' 
	AND I.TABLE_NAME = '%s' 
	-- 排除主键、唯一约束索引
	AND NOT EXISTS (
	SELECT
		1 
	FROM
		DBA_CONSTRAINTS C 
	WHERE
		I.INDEX_NAME = C.INDEX_NAME 
		AND I.TABLE_OWNER = C.OWNER 
		AND I.TABLE_NAME = C.TABLE_NAME 
		AND C.CONSTRAINT_TYPE IN ('P','U')
	)) temp
		GROUP BY
		temp.TABLE_OWNER,
		temp.TABLE_NAME,
		temp.UNIQUENESS,
		temp.INDEX_NAME,
		temp.INDEX_TYPE,
		temp.ITYP_OWNER,
		temp.ITYP_NAME,
		temp.PARAMETERS`,
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName))
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	temp.TABLE_NAME,
	temp.UNIQUENESS,
	temp.INDEX_NAME,
	temp.INDEX_TYPE,
	temp.ITYP_OWNER,
	temp.ITYP_NAME,
	temp.PARAMETERS,
	LISTAGG ( temp.COLUMN_NAME, ',' ) WITHIN GROUP ( ORDER BY temp.COLUMN_POSITION ) AS COLUMN_LIST 
FROM
	(
SELECT
		T.TABLE_OWNER,
		T.TABLE_NAME,
		I.UNIQUENESS,
		T.INDEX_NAME,
		I.INDEX_TYPE,
		NVL(I.ITYP_OWNER,'') ITYP_OWNER,
		NVL(I.ITYP_NAME,'') ITYP_NAME,
		NVL(I.PARAMETERS,'') PARAMETERS,
		DECODE((SELECT
	COUNT( 1 ) 
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = T.TABLE_OWNER
	AND S.TABLE_NAME = T.TABLE_NAME
	AND S.INDEX_OWNER = T.INDEX_OWNER
	AND S.INDEX_NAME = T.INDEX_NAME
	AND S.COLUMN_POSITION = T.COLUMN_POSITION),0,T.COLUMN_NAME, (SELECT
	xs.COLUMN_EXPRESSION
FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING ( SELECT DBMS_XMLGEN.GETXMLTYPE ( 
				q'[SELECT
	S.INDEX_OWNER,
	S.INDEX_NAME,
	S.COLUMN_EXPRESSION,
	S.COLUMN_POSITION
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = '%s'
	AND S.TABLE_NAME = '%s']' ) FROM DUAL ) COLUMNS 
	INDEX_OWNER VARCHAR2 ( 30 ) PATH 'INDEX_OWNER',
	INDEX_NAME VARCHAR2 ( 30 ) PATH 'INDEX_NAME',
	COLUMN_POSITION VARCHAR2 ( 30 ) PATH 'COLUMN_POSITION',
	COLUMN_EXPRESSION VARCHAR2 ( 4000 ) 
	) xs 
WHERE
xs.INDEX_OWNER = T.INDEX_OWNER
	AND xs.INDEX_NAME = T.INDEX_NAME
AND xs.COLUMN_POSITION = T.COLUMN_POSITION)) COLUMN_NAME,
		T.COLUMN_POSITION
	FROM
		DBA_IND_COLUMNS T,
		DBA_INDEXES I 
	WHERE t.table_owner=i.table_owner 
		AND t.table_name=i.table_name
		AND t.index_name=i.index_name
		AND i.uniqueness='NONUNIQUE'
	  	AND T.TABLE_OWNER = '%s'
	  	AND T.TABLE_NAME = '%s'
		-- 排除约束索引
		AND not exists (
		select 1 from DBA_CONSTRAINTS C where 
		c.owner=i.table_owner 
		and c.table_name=i.table_name
		and c.index_name=i.index_name)) temp		
	GROUP BY
		temp.TABLE_OWNER,
		temp.TABLE_NAME,
		temp.UNIQUENESS,
		temp.INDEX_NAME,
		temp.INDEX_TYPE,
		temp.ITYP_OWNER,
		temp.ITYP_NAME,
		temp.PARAMETERS`,
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName))
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
	cols, res, err := Query(e.OracleDB, `SELECT DISTINCT username FROM DBA_USERS`)
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
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`SELECT table_name AS TABLE_NAME FROM DBA_TABLES WHERE UPPER(owner) = UPPER('%s') AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, r := range res {
		tables = append(tables, strings.ToUpper(r["TABLE_NAME"]))
	}

	return tables, nil
}

func (e *Engine) GetOracleTableCollation(schemaName, schemaCollation string) (map[string]string, error) {
	var err error

	tablesMap := make(map[string]string)

	_, res, err := Query(e.OracleDB, fmt.Sprintf(`SELECT TABLE_NAME,DEFAULT_COLLATION FROM DBA_TABLES WHERE UPPER(owner) = UPPER('%s') AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tablesMap, err
	}
	for _, r := range res {
		if strings.ToUpper(r["DEFAULT_COLLATION"]) == utils.OracleUserTableColumnDefaultCollation {
			tablesMap[strings.ToUpper(r["TABLE_NAME"])] = strings.ToUpper(schemaCollation)
		} else {
			tablesMap[strings.ToUpper(r["TABLE_NAME"])] = strings.ToUpper(r["DEFAULT_COLLATION"])
		}
	}

	return tablesMap, nil
}

func (e *Engine) GetOracleTableType(schemaName string) (map[string]string, error) {
	tableMap := make(map[string]string)

	_, res, err := Query(e.OracleDB, fmt.Sprintf(`SELECT 
f.TABLE_NAME,
	(
	CASE WHEN f.CLUSTER_NAME IS NOT NULL THEN 'CLUSTERED' ELSE
		CASE	WHEN f.IOT_TYPE = 'IOT' THEN
			CASE WHEN t.IOT_TYPE != 'IOT' THEN t.IOT_TYPE ELSE 'IOT' 
				END 
		ELSE
				CASE	
						WHEN f.PARTITIONED = 'YES' THEN 'PARTITIONED' ELSE
					CASE
							WHEN f.TEMPORARY = 'Y' THEN 
							DECODE(f.DURATION,'SYS$SESSION','SESSION TEMPORARY','SYS$TRANSACTION','TRANSACTION TEMPORARY')
							ELSE 'HEAP' 
					END 
				END 
		END 
	END ) TABLE_TYPE 
FROM
(SELECT tmp.owner,tmp.TABLE_NAME，tmp.CLUSTER_NAME,tmp.PARTITIONED,tmp.TEMPORARY,tmp.DURATION,tmp.IOT_TYPE
FROM
	DBA_TABLES tmp, DBA_TABLES w
WHERE tmp.owner=w.owner AND tmp.table_name = w.table_name AND tmp.owner  = '%s' AND (w.IOT_TYPE IS NUll OR w.IOT_TYPE='IOT')) f left join (
select owner,iot_name,iot_type from DBA_TABLES WHERE owner  = '%s')t 
ON f.owner = t.owner AND f.table_name = t.iot_name`, strings.ToUpper(schemaName), strings.ToUpper(schemaName)))
	if err != nil {
		return tableMap, err
	}
	if len(res) == 0 {
		return tableMap, fmt.Errorf("oracle schema [%s] table type can't be null", schemaName)
	}

	for _, r := range res {
		if len(r) > 2 || len(r) == 0 || len(r) == 1 {
			return tableMap, fmt.Errorf("oracle schema [%s] table type values should be 2, result: %v", schemaName, r)
		}
		tableMap[r["TABLE_NAME"]] = r["TABLE_TYPE"]
	}

	return tableMap, nil
}

func (e *Engine) FilterOraclePartitionTable(schemaName string, tableSlice []string) ([]string, error) {
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`select table_name AS TABLE_NAME
  from dba_tables
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

func (e *Engine) FilterOracleTemporaryTable(schemaName string, tableSlice []string) ([]string, error) {
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`select table_name AS TABLE_NAME
  from dba_tables
 where TEMPORARY = 'Y'
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

func (e *Engine) FilterOracleClusteredTable(schemaName string, tableSlice []string) ([]string, error) {
	// 过滤蔟表
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`select table_name AS TABLE_NAME
  from dba_tables
 where CLUSTER_NAME IS NOT NULL
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

func (e *Engine) GetTiDBClusteredIndexValue() (string, error) {
	_, res, err := Query(e.MysqlDB, `select VARIABLE_VALUE from information_schema.session_variables where upper(variable_name) = upper('tidb_enable_clustered_index')`)
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", nil
	}
	return res[0]["VARIABLE_VALUE"], nil
}

func (e *Engine) GetTiDBAlterPKValue() (string, error) {
	_, res, err := Query(e.MysqlDB, `select VARIABLE_VALUE from information_schema.session_variables where upper(variable_name) = upper('tidb_config')`)
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", nil
	}
	return res[0]["VARIABLE_VALUE"], nil
}
