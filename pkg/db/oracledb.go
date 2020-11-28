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
package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "gopkg.in/rana/ora.v4"
)

func NewOracleDSN(dbUser, dbPassword, ipAddr, dbPort, dbName string) *Engine {
	dsn := fmt.Sprintf("%s/%s@%s:%s/%s", dbUser, dbPassword, ipAddr,
		dbPort, dbName)
	db, err := sql.Open("ora", dsn) // this does not really open a new connection
	if err != nil {
		log.Fatalf("Error on initializing database connection: %s", err.Error())
	}
	db.SetMaxIdleConns(100)

	err = db.Ping() // This DOES open a connection if necessary. This makes sure the database is accessible
	if err != nil {
		log.Fatalf("Error on opening database connection: %s", err.Error())
	}

	return &Engine{DB: db}
}

func (e *Engine) GetSchemaMeta() (schemaMeta []string) {
	querySQL := fmt.Sprintf("select DISTINCT username from all_users")
	cols, res, _ := GeneralQuery(e.DB, querySQL)
	for _, col := range cols {
		for _, r := range res {
			schemaMeta = append(schemaMeta, r[col])
		}
	}
	return
}

func (e *Engine) GetTableMeta(schemaName string) (tableMeta []map[string]string) {
	querySQL := fmt.Sprintf(`select table_name,table_type,comments 
from all_tab_comments 
where upper(owner)=upper('%s')`, strings.ToUpper(schemaName))
	_, tableMeta, _ = GeneralQuery(e.DB, querySQL)
	return
}

func (e *Engine) GetViewMeta(schemaName, viewName string) (viewMeta []map[string]string) {
	querySQL := fmt.Sprintf(`select TEXT 
from ALL_VIEWS 
where upper(view_name) = upper('%s') 
and upper(owner) = upper('%s')`, viewName, schemaName)
	_, viewMeta, _ = GeneralQuery(e.DB, querySQL)
	return
}

func (e *Engine) GetTableColumnMeta(schemaName string, tableName string) (colMeta []map[string]string) {
	querySQL := fmt.Sprintf(`select t.COLUMN_NAME,
	     t.DATA_TYPE,
	     t.DATA_LENGTH,
	     t.DATA_PRECISION,
	     t.DATA_SCALE,
	     t.NULLABLE,
	     t.DATA_DEFAULT,
	     c.COMMENTS
	from all_tab_columns t, all_col_comments c
	where t.table_name = c.table_name
	 and t.column_name = c.column_name
     and t.owner = c.owner
	 and t.table_name = upper('%s')
	 and t.owner = upper('%s')`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, colMeta, _ = GeneralQuery(e.DB, querySQL)

	return
}

func (e *Engine) GetTablePrimaryKey(schemaName string, tableName string) (pkList []map[string]string) {
	// for the primary key of an Engine table, you can use the following command to set whether the primary key takes effect.
	// disable the primary key: alter table tableName disable primary key;
	// enable the primary key: alter table tableName enable primary key;
	// primary key status Disabled will not do primary key processing
	querySQL := fmt.Sprintf(`select cu.constraint_name,
       LISTAGG(cu.column_name, ',') WITHIN GROUP(ORDER BY cu.POSITION) AS column_list
  from all_cons_columns cu, all_constraints au
 where cu.constraint_name = au.constraint_name
   and au.constraint_type = 'P'
   and au.STATUS = 'ENABLED'
   and upper(au.table_name) = upper('%s')
   and upper(cu.owner) = upper('%s')
 group by cu.constraint_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, pkList, _ = GeneralQuery(e.DB, querySQL)
	return
}

func (e *Engine) GetTableUniqueKey(schemaName string, tableName string) (ukList []map[string]string) {
	querySQL := fmt.Sprintf(`select cu.constraint_name,
       LISTAGG(cu.column_name, ',') WITHIN GROUP(ORDER BY cu.POSITION) AS column_list
  from all_cons_columns cu, all_constraints au
 where cu.constraint_name = au.constraint_name
   and au.constraint_type = 'U'
   and au.STATUS = 'ENABLED'
   and upper(au.table_name) = upper('%s')
   and upper(cu.owner) = upper('%s')
 group by cu.constraint_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, ukList, _ = GeneralQuery(e.DB, querySQL)
	return
}

func (e *Engine) GetTableForeignKey(schemaName string, tableName string) (fkList []map[string]string) {
	querySQL := fmt.Sprintf(`select t1.table_name,
       t2.table_name        as RTABLE_NAME,
       t1.constraint_name,
       -- t1.r_constraint_name as RCONSTRAINT_NAME,
       a1.column_name,
       a2.column_name       as RCOLUMN_NAME
  from all_constraints  t1,
       all_constraints  t2,
       all_cons_columns a1,
       all_cons_columns a2
 where t1.r_constraint_name = t2.constraint_name
   and t1.constraint_name = a1.constraint_name
   and t1.r_constraint_name = a2.constraint_name
  and upper(t1.table_name) = upper('%s')
   and upper(t1.owner) = upper('%s')`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, fkList, _ = GeneralQuery(e.DB, querySQL)
	return
}

func (e *Engine) GetTableIndexMeta(schemaName string, tableName string) (idxMeta []map[string]string) {
	querySQL := fmt.Sprintf(`select T.TABLE_NAME,
       I.UNIQUENESS, --是否唯一索引
       T.INDEX_NAME,
       --T.COLUMN_POSITION,
       LISTAGG(T.COLUMN_NAME, ',') WITHIN GROUP(ORDER BY T.COLUMN_POSITION) AS column_list
  FROM ALL_IND_COLUMNS T, ALL_INDEXES I, ALL_CONSTRAINTS C
 WHERE T.INDEX_NAME = I.INDEX_NAME
   AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)
   AND I.INDEX_TYPE != 'FUNCTION-BASED NORMAL' --排除基于函数的索引
   AND I.INDEX_TYPE != 'BITMAP' --排除位图索引
   AND C.CONSTRAINT_TYPE is Null --排除主键、唯一约束索引
   AND T.TABLE_NAME = upper('%s')
   AND T.TABLE_OWNER = upper('%s')
 group by T.TABLE_NAME,
          I.UNIQUENESS, --是否唯一索引
          T.INDEX_NAME`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, idxMeta, _ = GeneralQuery(e.DB, querySQL)
	return
}
