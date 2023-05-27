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
	"github.com/wentaojin/transferdb/common"
	"strings"
)

func (o *Oracle) GetOracleDBCharacterNLSSortCollation() (string, error) {
	querySQL := fmt.Sprintf(`SELECT VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_SORT'`)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (o *Oracle) GetOracleDBCharacterNLSCompCollation() (string, error) {
	querySQL := fmt.Sprintf(`SELECT VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP'`)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (o *Oracle) GetOracleDBVersion() (string, error) {
	querySQL := fmt.Sprintf(`select VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_RDBMS_VERSION'`)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res[0]["VALUE"], err
	}
	return res[0]["VALUE"], nil
}

func (o *Oracle) GetOracleDBCharacterSet() (string, error) {
	querySQL := fmt.Sprintf(`select userenv('language') AS LANG from dual`)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res[0]["LANG"], err
	}
	return res[0]["LANG"], nil
}

func (o *Oracle) GetOracleSchemaCollation(schemaName string) (string, error) {
	querySQL := fmt.Sprintf(`SELECT DECODE(DEFAULT_COLLATION,
'USING_NLS_COMP',(SELECT VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP'),DEFAULT_COLLATION) DEFAULT_COLLATION FROM DBA_USERS WHERE USERNAME = '%s'`, strings.ToUpper(schemaName))
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return "", err
	}
	return res[0]["DEFAULT_COLLATION"], nil
}

func (o *Oracle) GetOracleSchemaTableCollation(schemaName, schemaCollation string) (map[string]string, error) {
	var err error

	tablesMap := make(map[string]string)

	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT TABLE_NAME,DEFAULT_COLLATION FROM DBA_TABLES WHERE UPPER(owner) = UPPER('%s') AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tablesMap, err
	}
	for _, r := range res {
		if strings.ToUpper(r["DEFAULT_COLLATION"]) == common.OracleUserTableColumnDefaultCollation {
			tablesMap[strings.ToUpper(r["TABLE_NAME"])] = strings.ToUpper(schemaCollation)
		} else {
			tablesMap[strings.ToUpper(r["TABLE_NAME"])] = strings.ToUpper(r["DEFAULT_COLLATION"])
		}
	}

	return tablesMap, nil
}

func (o *Oracle) GetOracleSchemaTableType(schemaName string) (map[string]string, error) {
	tableMap := make(map[string]string)

	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT 
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
(SELECT tmp.owner,tmp.TABLE_NAME,tmp.CLUSTER_NAME,tmp.PARTITIONED,tmp.TEMPORARY,tmp.DURATION,tmp.IOT_TYPE
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

func (o *Oracle) IsOraclePartitionTable(schemaName, tableName string) (bool, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`select count(1) AS COUNT
  from dba_tables
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

func (o *Oracle) GetOraclePartitionTableINFO(schemaName, tableName string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT L.PARTITIONING_TYPE,
       L.SUBPARTITIONING_TYPE,
       L.PARTITION_EXPRESS,
       LISTAGG(skc.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY skc.COLUMN_POSITION) AS SUBPARTITION_EXPRESS
FROM (SELECT pt.OWNER,
             pt.TABLE_NAME,
             pt.PARTITIONING_TYPE,
             pt.SUBPARTITIONING_TYPE,
             LISTAGG(ptc.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY ptc.COLUMN_POSITION) AS PARTITION_EXPRESS
      FROM DBA_PART_TABLES pt,
           DBA_PART_KEY_COLUMNS ptc
      WHERE pt.OWNER = ptc.OWNER
        AND pt.TABLE_NAME = ptc.NAME
        AND ptc.OBJECT_TYPE = 'TABLE'
        AND UPPER(pt.OWNER) = UPPER('%s')
        AND UPPER(pt.TABLE_NAME) = UPPER('%s')
      GROUP BY pt.OWNER, pt.TABLE_NAME, pt.PARTITIONING_TYPE,
               pt.SUBPARTITIONING_TYPE) L,
     DBA_SUBPART_KEY_COLUMNS skc
WHERE L.OWNER = skc.OWNER
GROUP BY  L.PARTITIONING_TYPE,
       L.SUBPARTITIONING_TYPE,
       L.PARTITION_EXPRESS`, schemaName, tableName)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}
