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
)

func (e *Engine) GetOracleDBName() (string, string, string, error) {
	_, res, err := Query(e.OracleDB, `SELECT name dbname,platform_id platform_id,platform_name platform_name FROM v$database`)
	if err != nil {
		return "", "", "", err
	}
	return res[0]["DBNAME"], res[0]["PLATFORM_ID"], res[0]["PLATFORM_NAME"], nil
}

func (e *Engine) GetOracleGlobalName() (string, error) {
	_, res, err := Query(e.OracleDB, `SELECT global_name global_name FROM global_name`)
	if err != nil {
		return "", err
	}
	return res[0]["GLOBAL_NAME"], nil
}

func (e *Engine) GetOracleParameters() (string, string, string, string, error) {
	var (
		dbBlockSize             string
		clusterDatabase         string
		CLusterDatabaseInstance string
		characterSet            string
	)
	_, res, err := Query(e.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'db_block_size'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	dbBlockSize = res[0]["VALUE"]

	_, res, err = Query(e.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'cluster_database'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	clusterDatabase = res[0]["VALUE"]

	_, res, err = Query(e.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'cluster_database_instances'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	CLusterDatabaseInstance = res[0]["VALUE"]

	_, res, err = Query(e.OracleDB, `select VALUE from nls_database_parameters WHERE PARAMETER='NLS_CHARACTERSET'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	characterSet = res[0]["VALUE"]

	return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, nil
}

func (e *Engine) GetOracleInstance() ([]map[string]string, error) {
	_, res, err := Query(e.OracleDB, `SELECT
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

func (e *Engine) GetOracleDataTotal() (string, error) {
	_, res, err := Query(e.OracleDB, `SELECT
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

func (e *Engine) GetOracleNumCPU() (string, error) {
	_, res, err := Query(e.OracleDB, `SELECT VALUE FROM GV$OSSTAT where stat_name='NUM_CPUS'`)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (e *Engine) GetOracleMemoryGB() (string, error) {
	_, res, err := Query(e.OracleDB, `select value/1024/1024/1024 mem_gb from v$osstat where stat_name='PHYSICAL_MEMORY_BYTES'`)
	if err != nil {
		return "", err
	}
	return res[0]["MEM_GB"], nil
}

func (e *Engine) GetOracleSchemaOverview(schemaName []string) ([]map[string]string, error) {
	var (
		userSQL string
		vals    []map[string]string
	)
	if len(schemaName) == 0 {
		userSQL = `select username from dba_users where username NOT IN (
			'HR',
			'DVF',
			'DVSYS',
			'LBACSYS',
			'MDDATA',
			'OLAPSYS',
			'ORDPLUGINS',
			'ORDDATA',
			'MDSYS',
			'SI_INFORMTN_SCHEMA',
			'ORDSYS',
			'CTXSYS',
			'OJVMSYS',
			'WMSYS',
			'ANONYMOUS',
			'XDB',
			'GGSYS',
			'GSMCATUSER',
			'APPQOSSYS',
			'DBSNMP',
			'SYS$UMF',
			'ORACLE_OCM',
			'DBSFWUSER',
			'REMOTE_SCHEDULER_AGENT',
			'XS$NULL',
			'DIP',
			'GSMROOTUSER',
			'GSMADMIN_INTERNAL',
			'GSMUSER',
			'OUTLN',
			'SYSBACKUP',
			'SYSDG',
			'SYSTEM',
			'SYSRAC',
			'AUDSYS',
			'SYSKM',
			'SYS',
			'OGG',
			'SPA',
			'APEX_050000',
			'SQL_MONITOR',
			'APEX_030200',
			'SYSMAN',
			'EXFSYS',
			'OWBSYS_AUDIT',
			'FLOWS_FILES',
			'OWBSYS'
		)`
	} else {
		userSQL = fmt.Sprintf(`select username from dba_users where username IN (%s)`, strings.Join(schemaName, ","))
	}
	_, res, err := Query(e.OracleDB, userSQL)
	if err != nil {
		return vals, err
	}

	for _, val := range res {
		owner := fmt.Sprintf("'%s'", strings.ToUpper(val["USERNAME"]))
		_, tableRes, err := Query(e.OracleDB, fmt.Sprintf(`SELECT ROUND(SUM( bytes )/ 1024 / 1024 / 1024 ,2) GB 
FROM
	dba_segments 
WHERE
	OWNER IN (%s)  
	AND segment_type IN ( 'TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION' )`, owner))
		if err != nil {
			return vals, err
		}

		_, indexRes, err := Query(e.OracleDB, fmt.Sprintf(`SELECT ROUND(SUM( bytes )/ 1024 / 1024 / 1024 ,2) GB
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

		_, lobTable, err := Query(e.OracleDB, fmt.Sprintf(`SELECT ROUND(SUM( bytes )/ 1024 / 1024 / 1024 ,2) GB 
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
		_, lobIndex, err := Query(e.OracleDB, fmt.Sprintf(`SELECT ROUND(SUM( bytes )/ 1024 / 1024 / 1024 ,2) GB
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

		_, tableRows, err := Query(e.OracleDB, fmt.Sprintf(`select SUM(num_rows) NUM_ROWS from all_tables where OWNER IN (%s)`, owner))
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

func (e *Engine) GetOracleSchemaTableRowsTOP(schemaName []string) ([]map[string]string, error) {
	var (
		userSQL string
		vals    []map[string]string
	)
	if len(schemaName) == 0 {
		userSQL = `select username from dba_users where username NOT IN (
			'HR',
			'DVF',
			'DVSYS',
			'LBACSYS',
			'MDDATA',
			'OLAPSYS',
			'ORDPLUGINS',
			'ORDDATA',
			'MDSYS',
			'SI_INFORMTN_SCHEMA',
			'ORDSYS',
			'CTXSYS',
			'OJVMSYS',
			'WMSYS',
			'ANONYMOUS',
			'XDB',
			'GGSYS',
			'GSMCATUSER',
			'APPQOSSYS',
			'DBSNMP',
			'SYS$UMF',
			'ORACLE_OCM',
			'DBSFWUSER',
			'REMOTE_SCHEDULER_AGENT',
			'XS$NULL',
			'DIP',
			'GSMROOTUSER',
			'GSMADMIN_INTERNAL',
			'GSMUSER',
			'OUTLN',
			'SYSBACKUP',
			'SYSDG',
			'SYSTEM',
			'SYSRAC',
			'AUDSYS',
			'SYSKM',
			'SYS',
			'OGG',
			'SPA',
			'APEX_050000',
			'SQL_MONITOR',
			'APEX_030200',
			'SYSMAN',
			'EXFSYS',
			'OWBSYS_AUDIT',
			'FLOWS_FILES',
			'OWBSYS'
		)`
	} else {
		userSQL = fmt.Sprintf(`select username from dba_users where username IN (%s)`, strings.Join(schemaName, ","))
	}
	_, res, err := Query(e.OracleDB, userSQL)
	if err != nil {
		return vals, err
	}
	for _, val := range res {
		owner := fmt.Sprintf("'%s'", strings.ToUpper(val["USERNAME"]))
		_, tableRes, err := Query(e.OracleDB, fmt.Sprintf(`SELECT *
FROM
(
SELECT
OWNER,
SEGMENT_NAME,
SEGMENT_TYPE,
ROUND(SUM( bytes )/ 1024 / 1024 / 1024 ,2) GB
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

func (e *Engine) GetOracleObjectTypeOverview(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT OWNER,OBJECT_TYPE,count(*) AS COUNT
		FROM
		dba_objects
		WHERE
		OWNER NOT IN (
			'HR',
			'DVF',
			'DVSYS',
			'LBACSYS',
			'MDDATA',
			'OLAPSYS',
			'ORDPLUGINS',
			'ORDDATA',
			'MDSYS',
			'SI_INFORMTN_SCHEMA',
			'ORDSYS',
			'CTXSYS',
			'OJVMSYS',
			'WMSYS',
			'ANONYMOUS',
			'XDB',
			'GGSYS',
			'GSMCATUSER',
			'APPQOSSYS',
			'DBSNMP',
			'SYS$UMF',
			'ORACLE_OCM',
			'DBSFWUSER',
			'REMOTE_SCHEDULER_AGENT',
			'XS$NULL',
			'DIP',
			'GSMROOTUSER',
			'GSMADMIN_INTERNAL',
			'GSMUSER',
			'OUTLN',
			'SYSBACKUP',
			'SYSDG',
			'SYSTEM',
			'SYSRAC',
			'AUDSYS',
			'SYSKM',
			'SYS',
			'OGG',
			'SPA',
			'APEX_050000',
			'SQL_MONITOR',
			'APEX_030200',
			'SYSMAN',
			'EXFSYS',
			'OWBSYS_AUDIT',
			'FLOWS_FILES',
			'OWBSYS'
		)
		GROUP BY
		OWNER,OBJECT_TYPE
		ORDER BY COUNT DESC nulls last`
	} else {
		sql = fmt.Sprintf(`SELECT OWNER,OBJECT_TYPE,count(*) AS COUNT
		FROM
		dba_objects
		WHERE
		OWNER IN (%s)
		GROUP BY
		OWNER,OBJECT_TYPE ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOraclePartitionObjectType(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT DISTINCT
	OWNER,
	TABLE_NAME,
	PARTITIONING_TYPE,
	SUBPARTITIONING_TYPE 
FROM
	DBA_PART_TABLES 
WHERE
	OWNER NOT IN (
		'HR',
		'DVF',
		'DVSYS',
		'LBACSYS',
		'MDDATA',
		'OLAPSYS',
		'ORDPLUGINS',
		'ORDDATA',
		'MDSYS',
		'SI_INFORMTN_SCHEMA',
		'ORDSYS',
		'CTXSYS',
		'OJVMSYS',
		'WMSYS',
		'ANONYMOUS',
		'XDB',
		'GGSYS',
		'GSMCATUSER',
		'APPQOSSYS',
		'DBSNMP',
		'SYS$UMF',
		'ORACLE_OCM',
		'DBSFWUSER',
		'REMOTE_SCHEDULER_AGENT',
		'XS$NULL',
		'DIP',
		'GSMROOTUSER',
		'GSMADMIN_INTERNAL',
		'GSMUSER',
		'OUTLN',
		'SYSBACKUP',
		'SYSDG',
		'SYSTEM',
		'SYSRAC',
		'AUDSYS',
		'SYSKM',
		'SYS',
		'OGG',
		'SPA',
		'APEX_050000',
	'SQL_MONITOR' 
	)`
	} else {
		sql = fmt.Sprintf(`SELECT DISTINCT
	OWNER,
	TABLE_NAME,
	PARTITIONING_TYPE,
	SUBPARTITIONING_TYPE 
FROM
	DBA_PART_TABLES 
WHERE
	OWNER IN (%s)`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleSynonymObjectType(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select OWNER,count(*) COUNT from dba_synonyms where TABLE_OWNER not in  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') group by OWNER ORDER BY COUNT DESC`
	} else {
		sql = fmt.Sprintf(`select OWNER,count(*) COUNT from dba_synonyms where TABLE_OWNER IN (%s) group by OWNER ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleColumnTypeAndMaxLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select OWNER,DATA_TYPE,count(*) COUNT,max(data_length) max_data_length from dba_tab_columns where owner not in  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR')  group by owner,DATA_TYPE ORDER BY COUNT DESC`
	} else {
		sql = fmt.Sprintf(`select OWNER,DATA_TYPE,count(*) COUNT,max(data_length) max_data_length from dba_tab_columns where OWNER IN (%s)  group by owner,DATA_TYPE ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleIndexType(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select owner,INDEX_TYPE,count(*) COUNT from dba_indexes where owner not in  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') group by owner,INDEX_TYPE ORDER BY COUNT DESC`
	} else {
		sql = fmt.Sprintf(`select owner,INDEX_TYPE,count(*) COUNT from dba_indexes where OWNER IN (%s) group by owner,INDEX_TYPE ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleConstraintType(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select owner,CONSTRAINT_TYPE,count(*) COUNT from dba_constraints where owner not in  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') group by owner,CONSTRAINT_TYPE ORDER BY COUNT DESC`
	} else {
		sql = fmt.Sprintf(`select owner,CONSTRAINT_TYPE,count(*) COUNT from dba_constraints where OWNER IN (%s) group by owner,CONSTRAINT_TYPE ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleCodeObject(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select OWNER,NAME,TYPE,SUM(LINE) LINES from ALL_SOURCE where owner not in  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') GROUP BY OWNER,NAME,TYPE ORDER BY LINES DESC`
	} else {
		sql = fmt.Sprintf(`select OWNER,NAME,TYPE,SUM(LINE) LINES from ALL_SOURCE where OWNER IN (%s) GROUP BY OWNER,NAME,TYPE ORDER BY LINES DESC`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleAvgRowLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT ROWNUM,A.* FROM (select OWNER,TABLE_NAME,AVG_ROW_LEN FROM DBA_TABLES WHERE OWNER NOT IN  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR')   order by AVG_ROW_LEN desc nulls last)  A WHERE ROWNUM <=10`
	} else {
		sql = fmt.Sprintf(`SELECT ROWNUM,A.* FROM (select OWNER,TABLE_NAME,AVG_ROW_LEN FROM DBA_TABLES WHERE OWNER IN (%s) order by AVG_ROW_LEN desc nulls last)  A WHERE ROWNUM <=10`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTemporaryTable(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select OWNER,COUNT(*) COUNT FROM DBA_TABLES WHERE OWNER NOT IN  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR')  AND TEMPORARY='Y' GROUP BY OWNER ORDER BY COUNT DESC`
	} else {
		sql = fmt.Sprintf(`select OWNER,COUNT(*) COUNT FROM DBA_TABLES WHERE OWNER IN (%s) AND TEMPORARY='Y' GROUP BY OWNER ORDER BY COUNT DESC`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOraclePartitionTableOver1024(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select OWNER,TABLE_NAME,PARTITION_COUNT from DBA_PART_TABLES where OWNER not in  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') and PARTITION_COUNT>1024`
	} else {
		sql = fmt.Sprintf(`select OWNER,TABLE_NAME,PARTITION_COUNT from DBA_PART_TABLES where OWNER IN (%s) and PARTITION_COUNT>1024`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableRowLengthOver6M(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select OWNER,TABLE_NAME,AVG_ROW_LEN FROM DBA_TABLES WHERE OWNER NOT IN  ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR')  and AVG_ROW_LEN > 6 * 1024 * 1024`
	} else {
		sql = fmt.Sprintf(`select OWNER,TABLE_NAME,AVG_ROW_LEN FROM DBA_TABLES WHERE OWNER IN (%s) and AVG_ROW_LEN > 6 * 1024 * 1024`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableIndexLengthOver3072(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select INDEX_OWNER,INDEX_NAME,TABLE_NAME,sum(COLUMN_LENGTH) COUNT  from dba_ind_columns where TABLE_OWNER not in ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS') group by INDEX_OWNER,INDEX_NAME,TABLE_NAME  having sum(COLUMN_LENGTH) > 3072`
	} else {
		sql = fmt.Sprintf(`select INDEX_OWNER,INDEX_NAME,TABLE_NAME,sum(COLUMN_LENGTH) COUNT from dba_ind_columns where TABLE_OWNER IN (%s) group by INDEX_OWNER,INDEX_NAME,TABLE_NAME  having sum(COLUMN_LENGTH) > 3072`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableColumnCountsOver512(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT OWNER,TABLE_NAME,COUNT(COLUMN_NAME) COUNT FROM DBA_TAB_COLUMNS  WHERE  OWNER NOT IN ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') GROUP BY  OWNER,TABLE_NAME HAVING COUNT(COLUMN_NAME) > 512 ORDER BY OWNER,TABLE_NAME`
	} else {
		sql = fmt.Sprintf(`SELECT OWNER,TABLE_NAME,COUNT(COLUMN_NAME) COUNT FROM DBA_TAB_COLUMNS  WHERE  OWNER IN (%s) GROUP BY  OWNER,TABLE_NAME HAVING COUNT(COLUMN_NAME) > 512 ORDER BY OWNER,TABLE_NAME`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableIndexCountsOver64(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select TABLE_OWNER,TABLE_NAME,COUNT(INDEX_NAME) COUNT FROM dba_ind_columns WHERE TABLE_OWNER NOT IN ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') GROUP BY TABLE_OWNER,TABLE_NAME HAVING COUNT(INDEX_NAME) >64 ORDER BY TABLE_OWNER,TABLE_NAME`
	} else {
		sql = fmt.Sprintf(`select TABLE_OWNER,TABLE_NAME,COUNT(INDEX_NAME) COUNT FROM dba_ind_columns WHERE TABLE_OWNER IN (%s) GROUP BY TABLE_OWNER,TABLE_NAME HAVING COUNT(INDEX_NAME) >64 ORDER BY TABLE_OWNER,TABLE_NAME`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableNumberTypeCheck(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select OWNER,TABLE_NAME,COLUMN_NAME,NVL(DATA_PRECISION,0) DATA_PRECISION,NVL(DATA_SCALE,0) DATA_SCALE from dba_tab_columns where DATA_PRECISION is null and OWNER not in ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR')  and DATA_TYPE='NUMBER' and DATA_PRECISION is null ORDER BY OWNER,COLUMN_NAME`
	} else {
		sql = fmt.Sprintf(`select OWNER,TABLE_NAME,COLUMN_NAME,NVL(DATA_PRECISION,0) DATA_PRECISION,NVL(DATA_SCALE,0) DATA_SCALE from dba_tab_columns where DATA_PRECISION is null and OWNER IN (%s) and DATA_TYPE='NUMBER' and DATA_PRECISION is null ORDER BY OWNER,COLUMN_NAME`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleUsernameLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `select USERNAME,ACCOUNT_STATUS,CREATED from dba_users where username not in ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') AND length(USERNAME)>64`
	} else {
		sql = fmt.Sprintf(`select USERNAME,ACCOUNT_STATUS,CREATED from dba_users where username IN (%s) AND length(USERNAME)>64`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableNameLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT OWNER,TABLE_NAME FROM DBA_TABLES WHERE OWNER NOT IN ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') AND length(TABLE_NAME)>64 ORDER BY OWNER,TABLE_NAME`
	} else {
		sql = fmt.Sprintf(`SELECT OWNER,TABLE_NAME FROM DBA_TABLES WHERE OWNER IN (%s) AND length(TABLE_NAME)>64 ORDER BY OWNER,TABLE_NAME`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableColumnNameLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT OWNER,TABLE_NAME,COLUMN_NAME FROM DBA_TAB_COLUMNS WHERE OWNER NOT IN ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') AND length(COLUMN_NAME)>64 ORDER BY OWNER,TABLE_NAME,COLUMN_NAME`
	} else {
		sql = fmt.Sprintf(`SELECT OWNER,TABLE_NAME,COLUMN_NAME FROM DBA_TAB_COLUMNS WHERE OWNER IN (%s) AND length(COLUMN_NAME)>64 ORDER BY OWNER,TABLE_NAME,COLUMN_NAME`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableIndexNameLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT INDEX_OWNER,TABLE_NAME,INDEX_NAME FROM DBA_IND_COLUMNS WHERE TABLE_OWNER NOT IN ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') AND LENGTH(COLUMN_NAME) > 64 ORDER BY INDEX_OWNER,TABLE_NAME,INDEX_NAME`
	} else {
		sql = fmt.Sprintf(`SELECT INDEX_OWNER,TABLE_NAME,INDEX_NAME FROM DBA_IND_COLUMNS WHERE TABLE_OWNER IN (%s) AND LENGTH(COLUMN_NAME) > 64 ORDER BY INDEX_OWNER,TABLE_NAME,INDEX_NAME`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableViewNameLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT OWNER,VIEW_NAME,READ_ONLY FROM DBA_VIEWS WHERE OWNER NOT IN ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') AND LENGTH(VIEW_NAME) > 64 ORDER BY OWNER,VIEW_NAME`
	} else {
		sql = fmt.Sprintf(`SELECT OWNER,VIEW_NAME,READ_ONLY FROM DBA_VIEWS WHERE OWNER IN (%s) AND LENGTH(VIEW_NAME) > 64 ORDER BY OWNER,VIEW_NAME`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (e *Engine) GetOracleTableSequenceNameLength(schemaName []string) ([]map[string]string, error) {
	var sql string
	if len(schemaName) == 0 {
		sql = `SELECT SEQUENCE_OWNER,SEQUENCE_NAME,ORDER_FLAG FROM DBA_SEQUENCES WHERE SEQUENCE_OWNER NOT IN ('HR','DVF','DVSYS','LBACSYS','MDDATA','OLAPSYS','ORDPLUGINS','ORDDATA','MDSYS','SI_INFORMTN_SCHEMA','ORDSYS','CTXSYS','OJVMSYS','WMSYS','ANONYMOUS','XDB','GGSYS','GSMCATUSER','APPQOSSYS','DBSNMP','SYS$UMF','ORACLE_OCM','DBSFWUSER','REMOTE_SCHEDULER_AGENT','XS$NULL','DIP','GSMROOTUSER','GSMADMIN_INTERNAL','GSMUSER','OUTLN','SYSBACKUP','SYSDG','SYSTEM','SYSRAC','AUDSYS','SYSKM','SYS','OGG','SPA','APEX_050000','SQL_MONITOR') AND LENGTH(SEQUENCE_NAME)>64 ORDER BY SEQUENCE_OWNER,SEQUENCE_NAME,ORDER_FLAG`
	} else {
		sql = fmt.Sprintf(`SELECT SEQUENCE_OWNER,SEQUENCE_NAME,ORDER_FLAG FROM DBA_SEQUENCES WHERE SEQUENCE_OWNER IN (%s) AND LENGTH(SEQUENCE_NAME)>64 ORDER BY SEQUENCE_OWNER,SEQUENCE_NAME,ORDER_FLAG`, strings.Join(schemaName, ","))
	}

	_, res, err := Query(e.OracleDB, sql)
	if err != nil {
		return res, err
	}
	return res, nil
}
