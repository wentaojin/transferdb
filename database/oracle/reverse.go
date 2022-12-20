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
package oracle

import (
	"fmt"
	"regexp"
	"strings"
)

func (o *Oracle) GetOracleSchemaPartitionTable(schemaName string) ([]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT table_name AS TABLE_NAME
	FROM DBA_TABLES
 WHERE partitioned = 'YES'
   AND UPPER(owner) = UPPER('%s')`, schemaName))
	if err != nil {
		return []string{}, err
	}

	var tables []string
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return tables, nil
}

func (o *Oracle) GetOracleSchemaTemporaryTable(schemaName string) ([]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`select table_name AS TABLE_NAME
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
	return tables, nil
}

func (o *Oracle) GetOracleSchemaClusteredTable(schemaName string) ([]string, error) {
	// 过滤蔟表
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`select table_name AS TABLE_NAME
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
	return tables, nil
}

func (o *Oracle) GetOracleSchemaMaterializedView(schemaName string) ([]string, error) {
	// 过滤物化视图
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT OWNER,MVIEW_NAME FROM DBA_MVIEWS WHERE UPPER(OWNER) = UPPER('%s')`, schemaName))
	if err != nil {
		return []string{}, err
	}

	var tables []string
	for _, r := range res {
		tables = append(tables, r["MVIEW_NAME"])
	}
	return tables, nil
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

func (o *Oracle) GetOracleSchemaTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
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
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error) {
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
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableForeignKey(schemaName string, tableName string) ([]map[string]string, error) {
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
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableCheckKey(schemaName string, tableName string) ([]map[string]string, error) {
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
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
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
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error) {
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
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableComment(schemaName string, tableName string) ([]map[string]string, error) {
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
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return comments, err
	}
	if len(res) > 1 {
		return res, fmt.Errorf("oracle schema [%s] table [%s] comments exist multiple values: [%v]", schemaName, tableName, res)
	}

	return res, nil
}

func (o *Oracle) GetOracleSchemaTableColumn(schemaName string, tableName string, oraCollation bool) ([]map[string]string, error) {
	var querySQL string

	/*
			1、dataPrecision 精度范围 ORA-01727: numeric precision specifier is out of range (1 to 38)
			2、dataScale 精度范围 ORA-01728: numeric scale specifier is out of range (-84 to 127)
			3、oracle number 类型，desc tableName 表结构查看
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number
			- number -> number
		    - number(x,y) -> number(x,y)
			4、SQL 查询处理
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number(38,127)
			- number -> number(38,127)
			- number(x,y) -> number(x,y)
	*/
	if oraCollation {
		querySQL = fmt.Sprintf(`select t.COLUMN_NAME,
	    t.DATA_TYPE,
		 t.CHAR_LENGTH,
		 NVL(t.CHAR_USED,'UNKNOWN') CHAR_USED,
	    NVL(t.DATA_LENGTH,0) AS DATA_LENGTH,
	    DECODE(NVL(TO_CHAR(t.DATA_PRECISION),'*'),'*','38',TO_CHAR(t.DATA_PRECISION)) AS DATA_PRECISION,
	    DECODE(NVL(TO_CHAR(t.DATA_SCALE),'*'),'*','127',TO_CHAR(t.DATA_SCALE)) AS DATA_SCALE,
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
	    DECODE(NVL(TO_CHAR(t.DATA_PRECISION),'*'),'*','38',TO_CHAR(t.DATA_PRECISION)) AS DATA_PRECISION,
	    DECODE(NVL(TO_CHAR(t.DATA_SCALE),'*'),'*','127',TO_CHAR(t.DATA_SCALE)) AS DATA_SCALE,
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

	_, queryRes, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return queryRes, err
	}
	if len(queryRes) == 0 {
		return queryRes, fmt.Errorf("oracle table [%s.%s] column info cann't be null", schemaName, tableName)
	}

	// check constraints notnull
	// search_condition long datatype
	_, condRes, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT
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

func (o *Oracle) GetOracleSchemaTableColumnComment(schemaName string, tableName string) ([]map[string]string, error) {
	var querySQL string

	querySQL = fmt.Sprintf(`select t.COLUMN_NAME,
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

	_, queryRes, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return queryRes, err
	}
	if len(queryRes) == 0 {
		return queryRes, fmt.Errorf("oracle table [%s.%s] column info cann't be null", schemaName, tableName)
	}
	return queryRes, nil
}

func (o *Oracle) GetOracleExtendedMode() (bool, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `SELECT VALUE FROM V$PARAMETER WHERE UPPER(NAME) = UPPER('MAX_STRING_SIZE')`)
	if err != nil {
		return false, err
	}
	if strings.EqualFold(res[0]["VALUE"], "EXTENDED") {
		return true, nil
	}
	return false, nil
}
