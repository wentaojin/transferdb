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
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"strings"
)

type Postgres struct {
	Ctx    context.Context
	PGDB   *sql.DB
	GormDB *gorm.DB
}

func NewPostgresDB(ctx context.Context, mysqlDB *sql.DB, GormDB *gorm.DB) *Postgres {
	return &Postgres{
		Ctx:    ctx,
		PGDB:   mysqlDB,
		GormDB: GormDB,
	}
}

func Query(ctx context.Context, db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.QueryContext(ctx, querySQL)
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query failed: [%v]", querySQL, err.Error())
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Columns failed: [%v]", querySQL, err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("general sql [%v] query rows.Scan failed: [%v]", querySQL, err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			// 查询字段值 NULL
			// 如果字段值 = NULLABLE 则表示值是 NULL
			// 如果字段值 = "" 则表示值是空字符串
			// 如果字段值 = 'NULL' 则表示值是 NULL 字符串
			// 如果字段值 = 'null' 则表示值是 null 字符串
			if v == nil {
				row[cols[k]] = "NULLABLE"
			} else {
				// 处理空字符串以及其他值情况
				// 数据统一 string 格式显示
				row[cols[k]] = string(v)
			}
		}
		res = append(res, row)
	}

	if err = rows.Err(); err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Next failed: [%v]", querySQL, err.Error())
	}
	return cols, res, nil
}

func (p *Postgres) QuerySQL(querySQL string) (cols []string, res []map[string]string) {
	cols, res, _ = Query(p.Ctx, p.PGDB, querySQL)
	return
}

func (p *Postgres) GetSchemaMeta() (schemaMeta []string) {
	querySQL := fmt.Sprintf(`SELECT DISTINCT nspname 
FROM pg_namespace 
WHERE nspname not in ('pg_toast','pg_temp_1','pg_toast_temp_1','pg_catalog','information_schema')`)
	cols, res, _ := Query(p.Ctx, p.PGDB, querySQL)
	for _, col := range cols {
		for _, r := range res {
			schemaMeta = append(schemaMeta, r[col])
		}
	}
	return
}

func (p *Postgres) GetTableMeta(schemaName string) (tableMeta []map[string]string) {
	querySQL := fmt.Sprintf(` SELECT c.relname AS TABLE_NAME, 
                   CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' 
                   WHEN true THEN CASE 
                   WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind 
                    WHEN 'r' THEN 'SYSTEM TABLE' 
                    WHEN 'v' THEN 'SYSTEM VIEW' 
                    WHEN 'i' THEN 'SYSTEM INDEX' 
                    ELSE NULL 
                    END 
                   WHEN n.nspname = 'pg_toast' THEN CASE c.relkind 
                    WHEN 'r' THEN 'SYSTEM TOAST TABLE' 
                    WHEN 'i' THEN 'SYSTEM TOAST INDEX' 
                    ELSE NULL 
                    END 
                   ELSE CASE c.relkind 
                    WHEN 'r' THEN 'TEMPORARY TABLE' 
                    WHEN 'i' THEN 'TEMPORARY INDEX' 
                    WHEN 'S' THEN 'TEMPORARY SEQUENCE' 
                    WHEN 'v' THEN 'TEMPORARY VIEW' 
                    ELSE NULL 
                    END 
                   END 
                   WHEN false THEN CASE c.relkind 
                   WHEN 'r' THEN 'TABLE' 
                   WHEN 'i' THEN 'INDEX' 
                   WHEN 'S' THEN 'SEQUENCE' 
                   WHEN 'v' THEN 'VIEW' 
                   WHEN 'c' THEN 'TYPE' 
                   WHEN 'f' THEN 'FOREIGN TABLE' 
                   WHEN 'm' THEN 'MATERIALIZED VIEW' 
                   ELSE NULL 
                   END 
                   ELSE NULL 
                   END 
                   AS TABLE_TYPE, d.description AS REMARKS 
                   FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c 
                   LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) 
                   LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class') 
                   LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog') 
                   WHERE c.relnamespace = n.oid and c.relkind in('r','v') and n.nspname='%s'`, schemaName)
	_, tableMeta, _ = Query(p.Ctx, p.PGDB, querySQL)
	return
}

func (p *Postgres) GetViewMeta(schemaName, viewName string) (viewMeta []map[string]string) {
	querySQL := fmt.Sprintf(`SELECT
	schemaname,
	viewname,
	viewowner,
	definition 
FROM
	pg_views 
WHERE
	viewname = '%s' 
	AND schemaname = '%s'`, viewName, schemaName)
	_, viewMeta, _ = Query(p.Ctx, p.PGDB, querySQL)
	return
}

func (p *Postgres) GetTableColumnMeta(schemaName string, tableName string) (colMeta []map[string]string) {
	querySQL := fmt.Sprintf(`SELECT
	pcol.*,
	des.description 
FROM
	(
SELECT
	pc.oid AS oid,
--	col.table_schema,--	col.TABLE_NAME,
	col.ordinal_position AS ordinal_position,
	col.COLUMN_NAME AS COLUMN_NAME,
	col.data_type AS data_type,
	col.character_maximum_length AS character_maximum_length,
	col.numeric_precision AS numeric_precision,
	col.numeric_scale AS numeric_scale,
	col.is_nullable AS NULLABLE,
	col.column_default AS column_default 
FROM
	information_schema.COLUMNS col
	JOIN pg_class pc ON col.TABLE_NAME = pc.relname 
WHERE
	table_schema = '%s' 
	AND TABLE_NAME = '%s' 
ORDER BY
	ordinal_position 
	) pcol
	LEFT JOIN pg_description des ON pcol.oid = des.objoid 
	AND pcol.ordinal_position = des.objsubid`, strings.ToLower(schemaName), strings.ToLower(tableName))
	_, colMeta, _ = Query(p.Ctx, p.PGDB, querySQL)
	return colMeta
}

func (p *Postgres) GetTablePrimaryKey(schemaName string, tableName string) (pkList []map[string]string) {
	querySQL := fmt.Sprintf(`SELECT 
	-- n.nspname AS TABLE_SCHEM,
	-- ct.relname AS TABLE_NAME,
	-- A.attname AS COLUMN_NAME,
	-- ( i.keys ).n AS KEY_SEQ,
	ci.relname AS PK_NAME,
 string_agg(A.attname,',' order by ( i.keys ).n) AS COLUMN_LIST
FROM
	pg_catalog.pg_class ct
	JOIN pg_catalog.pg_attribute A ON ( ct.oid = A.attrelid )
	JOIN pg_catalog.pg_namespace n ON ( ct.relnamespace = n.oid )
	JOIN (
SELECT 
	i.indexrelid,
	i.indrelid,
	i.indisprimary,
	information_schema._pg_expandarray ( i.indkey ) AS keys 
FROM
	pg_catalog.pg_index i 
	) i ON ( A.attnum = ( i.keys ).x AND A.attrelid = i.indrelid )
	JOIN pg_catalog.pg_class ci ON ( ci.oid = i.indexrelid ) 
WHERE
TRUE 
	AND n.nspname = '%s' 
	AND ct.relname = '%s' 
	AND i.indisprimary 
GROUP BY
--	TABLE_NAME,
--	key_seq,
	pk_name`, strings.ToLower(schemaName), strings.ToLower(tableName))
	_, pkList, _ = Query(p.Ctx, p.PGDB, querySQL)
	return
}

func (p *Postgres) GetTableUniqueKey(schemaName string, tableName string) (ukList []map[string]string) {
	// tc.constraint_type support PRIMARY KEY、UNIQUE、FOREIGN KEY、CHECK
	querySQL := fmt.Sprintf(`SELECT
	tc.CONSTRAINT_NAME,
	string_agg ( kcu.COLUMN_NAME, ',' ORDER BY kcu.ordinal_position ) AS COLUMN_LIST 
FROM
	information_schema.table_constraints AS tc
	JOIN information_schema.key_column_usage AS kcu ON tc.CONSTRAINT_NAME = kcu.
	CONSTRAINT_NAME JOIN information_schema.constraint_column_usage AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
WHERE
	tc.constraint_type = 'UNIQUE' 
	AND tc.TABLE_NAME = '%s' 
	AND tc.table_schema = '%s' 
GROUP BY
	tc.CONSTRAINT_NAME`, strings.ToLower(tableName), strings.ToLower(schemaName))
	_, ukList, _ = Query(p.Ctx, p.PGDB, querySQL)
	return
}

func (p *Postgres) GetTableForeignKey(schemaName string, tableName string) (fkList []map[string]string) {
	querySQL := fmt.Sprintf(`SELECT
	tc.CONSTRAINT_NAME,
	tc.TABLE_NAME,
	kcu.COLUMN_NAME,
	ccu.TABLE_NAME AS foreign_table_name,
	ccu.COLUMN_NAME AS foreign_column_name 
FROM
	information_schema.table_constraints AS tc
	JOIN information_schema.key_column_usage AS kcu ON tc.CONSTRAINT_NAME = kcu.
	CONSTRAINT_NAME JOIN information_schema.constraint_column_usage AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
WHERE
	constraint_type = 'FOREIGN KEY' 
	AND tc.TABLE_NAME = '%s' 
	AND tc.table_schema = '%s'`, strings.ToLower(tableName), strings.ToLower(schemaName))
	_, fkList, _ = Query(p.Ctx, p.PGDB, querySQL)
	return
}

func (p *Postgres) GetTableIndexMeta(schemaName string, tableName string) (idxMeta []map[string]string) {
	querySQL := fmt.Sprintf(`SELECT T
	.relname AS TABLE_NAME,
	i.relname AS index_name,
	ix.indisunique AS is_unique, -- 是否唯一索引
--A.attname AS COLUMN_NAME,
	string_agg ( A.attname, ',' ORDER BY ( ix.keys ).n ) AS COLUMN_LIST 
FROM
	pg_class T,
	pg_class i,
	pg_namespace n,
	pg_attribute A,
	( SELECT indrelid, indexrelid, indisprimary, indisunique, information_schema._pg_expandarray ( indkey ) AS keys FROM pg_index ) ix 
WHERE
	T.oid = ix.indrelid 
	AND i.oid = ix.indexrelid 
	AND A.attrelid = T.oid 
	AND A.attnum = ( ix.keys ).x 
	AND A.attrelid = ix.indrelid 
	AND T.relnamespace = n.oid 
	AND ix.indisprimary = FALSE 
	AND T.relkind = 'r' 
	AND T.relname = '%s' 
	AND n.nspname = '%s' 
	AND i.relname NOT IN (
SELECT
	tc.CONSTRAINT_NAME 
FROM
	information_schema.table_constraints AS tc
	JOIN information_schema.key_column_usage AS kcu ON tc.CONSTRAINT_NAME = kcu.
	CONSTRAINT_NAME JOIN information_schema.constraint_column_usage AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
WHERE
	tc.constraint_type = 'UNIQUE' 
	AND tc.TABLE_NAME = '%s' 
	AND tc.table_schema = '%s' 
GROUP BY
	tc.CONSTRAINT_NAME 
	) 
GROUP BY
	TABLE_NAME,
	index_name,
	is_unique`, strings.ToLower(tableName), strings.ToLower(schemaName), strings.ToLower(tableName), strings.ToLower(schemaName))
	_, idxMeta, _ = Query(p.Ctx, p.PGDB, querySQL)
	return
}
