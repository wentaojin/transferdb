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
	"strings"
)

func (o *Oracle) GetOracleDBName() (string, string, string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `SELECT name dbname,platform_id platform_id,platform_name platform_name FROM v$database`)
	if err != nil {
		return "", "", "", err
	}
	return res[0]["DBNAME"], res[0]["PLATFORM_ID"], res[0]["PLATFORM_NAME"], nil
}

func (o *Oracle) GetOracleGlobalName() (string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `SELECT global_name global_name FROM global_name`)
	if err != nil {
		return "", err
	}
	return res[0]["GLOBAL_NAME"], nil
}

func (o *Oracle) GetOracleParameters() (string, string, string, string, error) {
	var (
		dbBlockSize             string
		clusterDatabase         string
		CLusterDatabaseInstance string
		characterSet            string
	)
	_, res, err := Query(o.Ctx, o.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'db_block_size'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	dbBlockSize = res[0]["VALUE"]

	_, res, err = Query(o.Ctx, o.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'cluster_database'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	clusterDatabase = res[0]["VALUE"]

	_, res, err = Query(o.Ctx, o.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'cluster_database_instances'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	CLusterDatabaseInstance = res[0]["VALUE"]

	_, res, err = Query(o.Ctx, o.OracleDB, `select VALUE from nls_database_parameters WHERE PARAMETER='NLS_CHARACTERSET'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	characterSet = res[0]["VALUE"]

	return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, nil
}

func (o *Oracle) GetOracleInstance() ([]map[string]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `SELECT
	host_name,
	instance_name,
	instance_number,
	thread# thread_number 
FROM
	v$instance`)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleDataTotal() (string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `SELECT
    (a.bytes - f.bytes)/1024/1024/1024 GB
FROM
    ( select sum(bytes) bytes
      from dba_data_files where tablespace_name not in('SYSTEM','SYSAUX')
     ) a
  , ( select sum(bytes) bytes
      from dba_free_space where tablespace_name not in('SYSTEM','SYSAUX')
     ) f`)
	if err != nil {
		return "", err
	}
	return res[0]["GB"], nil
}

func (o *Oracle) GetOracleNumCPU() (string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `SELECT VALUE FROM GV$OSSTAT where stat_name='NUM_CPUS'`)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (o *Oracle) GetOracleMemoryGB() (string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `select value/1024/1024/1024 mem_gb from v$osstat where stat_name='PHYSICAL_MEMORY_BYTES'`)
	if err != nil {
		return "", err
	}
	return res[0]["MEM_GB"], nil
}

func (o *Oracle) GetOracleMaxActiveSessionCount() ([]map[string]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, `select rownum,a.* from (
select /*+ parallel 8 */
dbid, instance_number, sample_id, sample_time, count(*) session_count
  from dba_hist_active_sess_history t
group by dbid, instance_number, sample_id, sample_time
order by session_count desc nulls last) a where rownum <=5`)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaOverview(schemaName []string) ([]map[string]string, error) {
	var (
		userSQL string
		vals    []map[string]string
	)

	userSQL = fmt.Sprintf(`select username from dba_users where username IN (%s)`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, userSQL)
	if err != nil {
		return vals, err
	}

	for _, val := range res {
		owner := fmt.Sprintf("'%s'", strings.ToUpper(val["USERNAME"]))
		_, tableRes, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT ROUND(NVL(SUM( bytes )/ 1024 / 1024 / 1024,0),2) GB 
FROM
	dba_segments 
WHERE
	OWNER IN (%s)  
	AND segment_type IN ( 'TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION' )`, owner))
		if err != nil {
			return vals, err
		}

		_, indexRes, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT ROUND(NVL(SUM( bytes )/ 1024 / 1024 / 1024,0),2) GB
FROM
	dba_indexes i,
	dba_segments s 
WHERE
	s.segment_name = i.index_name 
	AND s.OWNER = i.OWNER 
	AND s.OWNER IN (%s) 
	AND s.segment_type IN ( 'INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION' )`, owner))
		if err != nil {
			return vals, err
		}

		_, lobTable, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT ROUND(NVL(SUM( bytes )/ 1024 / 1024 / 1024,0) ,2) GB 
FROM
	dba_lobs l,
	dba_segments s 
WHERE
	s.segment_name = l.segment_name 
	AND s.OWNER = l.OWNER 
	AND s.OWNER IN (%s) 
	AND s.segment_type IN ( 'LOBSEGMENT', 'LOB PARTITION' ) `, owner))
		if err != nil {
			return vals, err
		}
		_, lobIndex, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT ROUND(NVL(SUM( bytes )/ 1024 / 1024 / 1024,0),2) GB
FROM
	dba_lobs l,
	dba_segments s 
WHERE
	s.segment_name = l.index_name 
	AND s.OWNER = l.OWNER 
	AND s.OWNER IN (%s) 
	AND s.segment_type = 'LOBINDEX'`, owner))

		if err != nil {
			return vals, err
		}

		_, tableRows, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`select NVL(SUM(num_rows),0) NUM_ROWS from dba_tables where OWNER IN (%s)`, owner))
		if err != nil {
			return vals, err
		}

		if tableRes[0]["GB"] == "" {
			tableRes[0]["GB"] = "0"
		}
		if indexRes[0]["GB"] == "" {
			indexRes[0]["GB"] = "0"
		}
		if lobTable[0]["GB"] == "" {
			lobTable[0]["GB"] = "0"
		}
		if lobIndex[0]["GB"] == "" {
			lobIndex[0]["GB"] = "0"
		}

		vals = append(vals, map[string]string{
			"SCHEMA":   strings.ToUpper(val["USERNAME"]),
			"TABLE":    tableRes[0]["GB"],
			"INDEX":    indexRes[0]["GB"],
			"LOBTABLE": lobTable[0]["GB"],
			"LOBINDEX": lobIndex[0]["GB"],
			"ROWCOUNT": tableRows[0]["NUM_ROWS"],
		})
	}
	return vals, nil
}

func (o *Oracle) GetOracleSchemaTableRowsTOP(schemaName []string) ([]map[string]string, error) {
	var (
		userSQL string
		vals    []map[string]string
	)

	userSQL = fmt.Sprintf(`select username from dba_users where username IN (%s)`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, userSQL)
	if err != nil {
		return vals, err
	}
	for _, val := range res {
		owner := fmt.Sprintf("'%s'", strings.ToUpper(val["USERNAME"]))
		_, tableRes, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT *
FROM
(
SELECT
OWNER,
SEGMENT_NAME,
SEGMENT_TYPE,
ROUND(NVL(SUM( bytes )/ 1024 / 1024 / 1024 ,0),2) GB
	FROM
		dba_segments 
	WHERE
		OWNER IN (%s) 
		AND segment_type IN (
			'TABLE',
			'TABLE PARTITION',
		'TABLE SUBPARTITION' 
	)
	GROUP BY OWNER,SEGMENT_NAME,SEGMENT_TYPE
ORDER BY GB DESC
) WHERE ROWNUM <= 10`, owner))
		if err != nil {
			return vals, err
		}
		for _, res := range tableRes {
			vals = append(vals, map[string]string{
				"SCHEMA":       strings.ToUpper(val["USERNAME"]),
				"SEGMENT_NAME": res["SEGMENT_NAME"],
				"SEGMENT_TYPE": res["SEGMENT_TYPE"],
				"TABLE_SIZE":   res["GB"],
			})
		}

	}
	return vals, err
}

func (o *Oracle) GetOracleSchemaCodeObject(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,NAME,TYPE,MAX(LINE) LINES from DBA_SOURCE where OWNER IN (%s) GROUP BY OWNER,NAME,TYPE ORDER BY LINES DESC`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaPartitionObjectType(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT DISTINCT
	OWNER,
	TABLE_NAME,
	PARTITIONING_TYPE,
	SUBPARTITIONING_TYPE 
FROM
	DBA_PART_TABLES 
WHERE
	OWNER IN (%s)`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableAvgRowLengthTOP(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT ROWNUM,A.* FROM (select OWNER,TABLE_NAME,AVG_ROW_LEN FROM DBA_TABLES WHERE OWNER IN (%s) order by AVG_ROW_LEN desc nulls last)  A WHERE ROWNUM <=10`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaSynonymObject(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,SYNONYM_NAME,TABLE_OWNER,TABLE_NAME FROM dba_synonyms WHERE TABLE_OWNER IN (%s)`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaMaterializedViewObject(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,MVIEW_NAME,REWRITE_CAPABILITY,REFRESH_MODE,REFRESH_METHOD,FAST_REFRESHABLE FROM DBA_MVIEWS WHERE OWNER IN (%s)`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaPartitionTableCountsOver1024(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select OWNER,TABLE_NAME,PARTITION_COUNT from DBA_PART_TABLES where OWNER IN (%s) and PARTITION_COUNT>1024`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableRowLengthOver6M(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select OWNER,TABLE_NAME,ROUND(AVG_ROW_LEN/1024/1024,2) AS AVG_ROW_LEN FROM DBA_TABLES WHERE OWNER IN (%s) and ROUND(AVG_ROW_LEN/1024/1024,2) >= 6`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableIndexLengthOver3072(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select INDEX_OWNER,INDEX_NAME,TABLE_NAME,sum(COLUMN_LENGTH)*4 LENGTH_OVER
  from dba_ind_columns where TABLE_OWNER IN (%s) group by INDEX_OWNER,INDEX_NAME,TABLE_NAME HAVING sum(COLUMN_LENGTH)*4 > 3072`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableColumnCountsOver512(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,TABLE_NAME,COUNT(COLUMN_NAME) COUNT_OVER FROM DBA_TAB_COLUMNS  WHERE  OWNER IN (%s) GROUP BY  OWNER,TABLE_NAME HAVING COUNT(COLUMN_NAME) > 512 ORDER BY OWNER,TABLE_NAME`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableIndexCountsOver64(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select TABLE_OWNER,TABLE_NAME,COUNT(INDEX_NAME) COUNT_OVER FROM dba_ind_columns WHERE TABLE_OWNER IN (%s) GROUP BY TABLE_OWNER,TABLE_NAME HAVING COUNT(INDEX_NAME) >64 ORDER BY TABLE_OWNER,TABLE_NAME`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableNumberTypeEqual0(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select OWNER,TABLE_NAME,COLUMN_NAME,NVL(DATA_PRECISION,0) DATA_PRECISION,NVL(DATA_SCALE,0) DATA_SCALE from dba_tab_columns where DATA_PRECISION is null and OWNER IN (%s) and DATA_TYPE='NUMBER' and DATA_PRECISION is null ORDER BY OWNER,COLUMN_NAME`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleUsernameLengthOver64(schemaName []string) ([]map[string]string, error) {

	querySQL := fmt.Sprintf(`select USERNAME,ACCOUNT_STATUS,CREATED,length(USERNAME) LENGTH_OVER from dba_users where username IN (%s) AND length(USERNAME) > 64`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableNameLengthOver64(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,TABLE_NAME,length(TABLE_NAME) LENGTH_OVER FROM DBA_TABLES WHERE OWNER IN (%s) AND length(TABLE_NAME) > 64 ORDER BY OWNER,TABLE_NAME`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableColumnNameLengthOver64(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,TABLE_NAME,COLUMN_NAME,length(COLUMN_NAME) LENGTH_OVER FROM DBA_TAB_COLUMNS WHERE OWNER IN (%s) AND length(COLUMN_NAME) >64 ORDER BY OWNER,TABLE_NAME,COLUMN_NAME`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableIndexNameLengthOver64(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT INDEX_OWNER,TABLE_NAME,INDEX_NAME,LENGTH(COLUMN_NAME) LENGTH_OVER
 FROM DBA_IND_COLUMNS WHERE TABLE_OWNER IN (%s) AND LENGTH(COLUMN_NAME) > 64 ORDER BY INDEX_OWNER,TABLE_NAME,INDEX_NAME`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableViewNameLengthOver64(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,VIEW_NAME,READ_ONLY,LENGTH(VIEW_NAME) LENGTH_OVER FROM DBA_VIEWS WHERE OWNER IN (%s) AND LENGTH(VIEW_NAME) > 64 ORDER BY OWNER,VIEW_NAME`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableSequenceNameLengthOver64(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT SEQUENCE_OWNER,SEQUENCE_NAME,ORDER_FLAG,LENGTH(SEQUENCE_NAME) LENGTH_OVER FROM DBA_SEQUENCES WHERE SEQUENCE_OWNER IN (%s) AND LENGTH(SEQUENCE_NAME) > 64 ORDER BY SEQUENCE_OWNER,SEQUENCE_NAME,ORDER_FLAG`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTableTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT 
temp.SCHEMA_NAME,
temp.TABLE_TYPE,
SUM(temp.OBJECT_SIZE) AS OBJECT_SIZE,
COUNT(1) AS COUNTS
FROM
(SELECT 
f.OWNER AS SCHEMA_NAME,
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
	END ) TABLE_TYPE,
    ROUND(NVL(f.NUM_ROWS * f.AVG_ROW_LEN / 1024 / 1024 / 1024,0),2) AS OBJECT_SIZE
FROM
(SELECT tmp.owner,tmp.NUM_ROWS,tmp.AVG_ROW_LEN,tmp.TABLE_NAME，tmp.CLUSTER_NAME,tmp.PARTITIONED,tmp.TEMPORARY,tmp.DURATION,tmp.IOT_TYPE
FROM
	DBA_TABLES tmp, DBA_TABLES w
WHERE tmp.owner=w.owner AND tmp.table_name = w.table_name AND tmp.owner in (%s) AND (w.IOT_TYPE IS NUll OR w.IOT_TYPE='IOT')) f left join (
select owner,iot_name,iot_type from DBA_TABLES WHERE owner in (%s)) t 
ON f.owner = t.owner AND f.table_name = t.iot_name) temp
GROUP BY temp.SCHEMA_NAME,temp.TABLE_TYPE`, strings.Join(schemaName, ","), strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaColumnDataDefaultCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT 
	xs.OWNER,
	xs.DATA_DEFAULT,
	COUNT(1) COUNTS
	FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING ( SELECT DBMS_XMLGEN.GETXMLTYPE (
				q'[select t.OWNER,
	    t.DATA_DEFAULT
	from dba_tab_columns t, dba_col_comments c
	where t.table_name = c.table_name
	and t.column_name = c.column_name
	and t.owner = c.owner
	and upper(t.owner) IN (%s)]' ) FROM DUAL ) COLUMNS OWNER VARCHAR2 (300) PATH 'OWNER',
		DATA_DEFAULT VARCHAR2 ( 4000 )
	) xs
	GROUP BY xs.OWNER,xs.DATA_DEFAULT`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaViewTypeCounts(schemaName []string) ([]map[string]string, error) {
	// Type of the view if the view is a typed view -> view_type
	querySQL := fmt.Sprintf(`SELECT OWNER,NVL(VIEW_TYPE,'VIEW') VIEW_TYPE,VIEW_TYPE_OWNER,COUNT(1) AS COUNTS FROM DBA_VIEWS WHERE OWNER IN (%s) GROUP BY OWNER,VIEW_TYPE,VIEW_TYPE_OWNER`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaObjectTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,OBJECT_TYPE,COUNT(1) COUNTS FROM DBA_OBJECTS WHERE OWNER IN (%s) AND OBJECT_TYPE NOT IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION','INDEX','VIEW') GROUP BY OWNER,OBJECT_TYPE`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaPartitionTypeCounts(schemaName []string) ([]map[string]string, error) {
	// SUBPARTITIONING_TYPE = 'NONE' -> remove subpartition
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	PARTITIONING_TYPE,
	COUNT(1) COUNTS
FROM
	DBA_PART_TABLES 
WHERE
	OWNER IN (%s)
AND SUBPARTITIONING_TYPE = 'NONE'
GROUP BY OWNER,PARTITIONING_TYPE`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaSubPartitionTypeCounts(schemaName []string) ([]map[string]string, error) {
	// SUBPARTITIONING_TYPE <> 'NONE'  -> remove normal partition
	querySQL := fmt.Sprintf(`SELECT 
	OWNER,
	SUBPARTITIONING_TYPE,
	COUNT(1) COUNTS
FROM
(SELECT
	OWNER,
	PARTITIONING_TYPE || '-' || SUBPARTITIONING_TYPE AS SUBPARTITIONING_TYPE
FROM
	DBA_PART_TABLES 
WHERE
	OWNER IN (%s)
AND SUBPARTITIONING_TYPE <> 'NONE'
)
GROUP BY OWNER,SUBPARTITIONING_TYPE`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaTemporaryTableTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select OWNER,DURATION AS TEMP_TYPE,COUNT(*) COUNT FROM DBA_TABLES WHERE OWNER IN (%s) AND TEMPORARY='Y' AND DURATION IS NOT NULL GROUP BY OWNER,DURATION`, strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaConstraintTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select owner,CONSTRAINT_TYPE,count(*) COUNT from dba_constraints where OWNER IN (%s) group by owner,CONSTRAINT_TYPE ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaIndexTypeCounts(schemaName []string) ([]map[string]string, error) {
	// 排除 LOB INDEX，LOB INDEX 用于 LOB 字段，Oracle 数据库自动为该字段创建的索引
	querySQL := fmt.Sprintf(`select TABLE_OWNER,INDEX_TYPE,COUNT(*) COUNT from (
select distinct a.table_owner,a.table_name,a.index_owner,a.index_name,INDEX_TYPE
from dba_ind_columns a,dba_indexes b
where a.table_owner=b.table_owner and a.table_name=b.table_name 
and a.index_owner=b.owner
and a.table_owner in (%s)
and b.index_type<>'LOB' and a.index_name=b.index_name
and a.INDEX_NAME not in (select INDEX_NAME from dba_lobs where owner in (%s))
) group by table_owner,index_type order by COUNT DESC`, strings.Join(schemaName, ","), strings.Join(schemaName, ","))

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) GetOracleSchemaColumnTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`select OWNER,DATA_TYPE,count(*) COUNT,
	CASE 
	WHEN DATA_TYPE = 'NUMBER' THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_PRECISION),'*'),'*','38',TO_CHAR(DATA_PRECISION))))
	WHEN regexp_like(DATA_TYPE,'INTERVAL YEAR.*') THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_PRECISION),'*'),'*','38',TO_CHAR(DATA_PRECISION))))
	WHEN regexp_like(DATA_TYPE,'INTERVAL DAY.*') THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_SCALE),'*'),'*','127',TO_CHAR(DATA_SCALE))))
	WHEN regexp_like(DATA_TYPE,'TIMESTAMP.*') THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_SCALE),'*'),'*','127',TO_CHAR(DATA_SCALE))))
	ELSE
	 MAX(DATA_LENGTH)
	END AS MAX_DATA_LENGTH from dba_tab_columns where OWNER IN (%s)  group by OWNER,DATA_TYPE ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}
