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
	"context"
	"fmt"
	"strconv"
	"strings"
)

func (e *Engine) GetOracleRedoLogSCN(scn int) (int, error) {
	querySQL := fmt.Sprintf(`select  FIRST_CHANGE# AS SCN
  from (SELECT  GROUP#,
               first_change#,
               MEMBERS,
               FIRST_TIME
          FROM v$LOG
         WHERE FIRST_CHANGE# <= %d
         order by FIRST_CHANGE# desc)
 where rownum = 1`, scn)
	_, res, err := Query(e.OracleDB, querySQL)
	var globalSCN int
	if err != nil {
		return globalSCN, err
	}
	if len(res) > 0 {
		globalSCN, err = strconv.Atoi(res[0]["SCN"])
		if err != nil {
			return globalSCN, err
		}
	}
	return globalSCN, nil
}

func (e *Engine) GetOracleArchivedLogSCN(scn int) (int, error) {
	querySQL := fmt.Sprintf(`select FIRST_CHANGE# AS SCN
  from (select FIRST_CHANGE#,FIRST_TIME
          from v$archived_log
         where STATUS = 'A'
           and DELETED='NO'
           and FIRST_CHANGE# <= %d
         order by FIRST_CHANGE# desc)
 where rownum = 1`, scn)
	var globalSCN int
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return globalSCN, err
	}
	if len(res) > 0 {
		globalSCN, err = strconv.Atoi(res[0]["SCN"])
		if err != nil {
			return globalSCN, err
		}
	}
	return globalSCN, nil
}

func (e *Engine) GetOracleRedoLogFile(scn int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT 
       --l.GROUP# GROUP_NUMBER,
       l.FIRST_CHANGE# AS FIRST_CHANGE,
       --l.BYTES / 1024 / 1024 AS LOG_SIZE,
       --L.NEXT_CHANGE# AS NEXT_CHANGE,
       lf.MEMBER LOG_FILE
  FROM v$LOGFILE lf, v$LOG l
 WHERE l.GROUP# = lf.GROUP#
   AND l.FIRST_CHANGE# >= %d
 ORDER BY l.FIRST_CHANGE# ASC`, scn)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return []map[string]string{}, err
	}
	return res, nil
}

func (e *Engine) GetOracleArchivedLogFile(scn int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT NAME AS LOG_FILE,
       FIRST_CHANGE# AS FIRST_CHANGE,
       --NEXT_CHANGE# AS NEXT_CHANGE,
       --BLOCKS * BLOCK_SIZE / 1024 / 1024 AS LOG_SIZE
  FROM v$ARCHIVED_LOG
 WHERE STATUS = 'A'
   AND DELETED = 'NO'
   AND FIRST_CHANGE# >= %d
 ORDER BY FIRST_CHANGE# ASC`, scn)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return []map[string]string{}, err
	}
	return res, nil
}

func (e *Engine) GetMySQLTableIncrementMetaMinSCNTime(metaSchemaName string) (int, error) {
	querySQL := fmt.Sprintf(`SELECT DISTINCT MIN(global_scn) GLOBAL_SCN
  FROM %s.table_increment_meta`, metaSchemaName)

	_, res, err := Query(e.MysqlDB, querySQL)
	if err != nil {
		return 0, err
	}
	var globalSCN int
	if len(res) > 0 {
		globalSCN, err = strconv.Atoi(res[0]["GLOBAL_SCN"])
		if err != nil {
			return globalSCN, err
		}
		return globalSCN, nil
	}
	return globalSCN, nil
}

func (e *Engine) GetMySQLTableIncrementMetaRecord(metaSchemaName, sourceSchemaName string) ([]string, error) {
	querySQL := fmt.Sprintf(`SELECT source_table_name AS SOURCE_TABLE_NAME
  FROM %s.table_increment_meta WHERE upper(source_schema_name) = upper('%s')`, metaSchemaName, sourceSchemaName)

	_, res, err := Query(e.MysqlDB, querySQL)
	if err != nil {
		return []string{}, err
	}

	var tableSlice []string
	for _, r := range res {
		tableSlice = append(tableSlice, r["SOURCE_TABLE_NAME"])
	}
	return tableSlice, nil
}

func (e *Engine) AddOracleLogminerlogFile(logFile string) error {
	addLogFile := fmt.Sprintf(`BEGIN
  dbms_logmnr.add_logfile(logfilename => '%s',
                          options     => dbms_logmnr.NEW);
END;`, logFile)

	ctx, _ := context.WithCancel(context.Background())
	_, err := e.OracleDB.ExecContext(ctx, addLogFile)
	if err != nil {
		return fmt.Errorf("oracle logminer add log file failed: %v", err)
	}
	return nil
}
func (e *Engine) StartOracleLogminerStoredProcedure(scn int) error {
	startLogminer := fmt.Sprintf(`BEGIN
  dbms_logmnr.start_logmnr(startSCN => %d,
                           options  => SYS.DBMS_LOGMNR.SKIP_CORRUPTION +       -- 日志遇到坏块，不报错退出，直接跳过
                                       SYS.DBMS_LOGMNR.NO_SQL_DELIMITER +
                                       SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT +
                                       --SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY +  -- UPDATE 语句只显示被提交且修改的字段数据，未修改的字段数据不显示
                                       SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                       SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);
END;`, scn)

	ctx, _ := context.WithCancel(context.Background())
	_, err := e.OracleDB.ExecContext(ctx, startLogminer)
	if err != nil {
		return fmt.Errorf("oracle logminer stored procedure start failed: %v", err)
	}
	return nil
}

func (e *Engine) EndOracleLogminerStoredProcedure() error {
	endLogminer := fmt.Sprintf(`BEGIN
  dbms_logmnr.end_logmnr();
END;`)
	ctx, _ := context.WithCancel(context.Background())
	_, err := e.OracleDB.ExecContext(ctx, endLogminer)
	if err != nil {
		return fmt.Errorf("oracle logminer stored procedure end failed: %v", err)
	}
	return nil
}

// 获取 Oracle logminer 日志内容并过滤筛选已提交的 INSERT/DELETE/UPDATE 事务语句
// 考虑异构数据库 MySQL，只同步 INSERT/DELETE/UPDATE 事务语句以及 TRUNCATE TABLE/DROP TABLE DDL 语句，其他类型 SQL 不同步
// V$LOGMNR_CONTENTS 字段解释参考链接
// https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-LOGMNR_CONTENTS.html#GUID-B9196942-07BF-4935-B603-FA875064F5C3
func (e *Engine) GetOracleLogminerContentToMySQL(schemaName string, tableSlice []string) ([]map[string]string, error) {
	var tbls []string
	for _, tbl := range tableSlice {
		tbls = append(tbls, fmt.Sprintf(`'%s'`, tbl))
	}
	querySQL := fmt.Sprintf(`SELECT SCN, SEG_OWNER, SEG_NAME, SQL_REDO, OPERATION, --SQL_UNDO, 
  FROM V$LOGMNR_CONTENTS
 WHERE 1 = 1
   AND UPPER(SEG_OWNER) = UPPER('%s')
   AND UPPER(SEG_NAME) IN (%s')
   AND OPERATION IN ('INSERT', 'DELETE','UPDATE','DDL')
 ORDER BY SCN`, schemaName, strings.ToUpper(strings.Join(tbls, ",")))

	_, rowsResult, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return rowsResult, err
	}

	return rowsResult, nil
}
