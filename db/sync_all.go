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
	"time"

	"go.uber.org/zap"

	"github.com/WentaoJin/transferdb/zlog"

	"github.com/WentaoJin/transferdb/util"
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
       --NEXT_CHANGE# AS NEXT_CHANGE,
       --BLOCKS * BLOCK_SIZE / 1024 / 1024 AS LOG_SIZE,
       FIRST_CHANGE# AS FIRST_CHANGE
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

func (e *Engine) GetMySQLTableIncrementMetaMinGlobalSCNTime(metaSchemaName string) (int, error) {
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

func (e *Engine) GetMySQLTableIncrementMetaSingleSourceTableSCNTime(sourceSchemaName,
	sourceTableName string) (TableIncrementMeta, error) {
	var (
		incrementMeta TableIncrementMeta
	)
	if err := e.GormDB.Where("upper(source_schema_name) = ? AND upper(source_table_name) = ?",
		strings.ToUpper(sourceSchemaName), strings.ToUpper(sourceTableName)).
		Find(&incrementMeta).Error; err != nil {
		return incrementMeta, err
	}
	return incrementMeta, nil
}

func (e *Engine) GetMySQLTableIncrementMetaRecord(sourceSchemaName string) ([]string, map[string]int, error) {
	var (
		incrementMeta      []TableIncrementMeta
		tableMetas         map[string]int
		transferTableSlice []string
	)
	if err := e.GormDB.Where("upper(source_schema_name) = ?", strings.ToUpper(sourceSchemaName)).
		Find(&incrementMeta).Error; err != nil {
		return transferTableSlice, tableMetas, err
	}

	if len(incrementMeta) == 0 {
		return transferTableSlice, tableMetas, fmt.Errorf("mysql increment mete table [table_increment_meta] can't null")
	}

	tableMetas = make(map[string]int)
	for _, tbl := range incrementMeta {
		tableMetas[strings.ToUpper(tbl.SourceTableName)] = tbl.SourceTableSCN
		transferTableSlice = append(transferTableSlice, strings.ToUpper(tbl.SourceTableName))
	}
	return transferTableSlice, tableMetas, nil
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
                                       SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY +
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
type LogminerContent struct {
	SCN       int
	SegOwner  string
	TableName string
	SQLRedo   string
	SQLUndo   string
	Operation string
}

func (e *Engine) GetOracleLogminerContentToMySQL(schemaName string, sourceTableName string, lastCheckpoint int) ([]LogminerContent, error) {
	var lcs []LogminerContent

	ctx, cancel := context.WithTimeout(context.Background(), util.LogminerQueryTimeout)
	defer cancel()

	querySQL := fmt.Sprintf(`SELECT SCN,
       SEG_OWNER,
       TABLE_NAME,
       SQL_REDO,
       SQL_UNDO,
       OPERATION
  FROM V$LOGMNR_CONTENTS
 WHERE 1 = 1
   AND UPPER(SEG_OWNER) = '%s'
   AND UPPER(TABLE_NAME) = '%s'
   AND OPERATION IN ('INSERT', 'DELETE', 'UPDATE', 'DDL')
   AND SCN >= %d 
 ORDER BY SCN`, strings.ToUpper(schemaName), strings.ToUpper(sourceTableName), lastCheckpoint)

	startTime := time.Now()
	zlog.Logger.Info("logminer sql",
		zap.String("sql", querySQL),
		zap.Time("start time", startTime))

	rows, err := e.OracleDB.QueryContext(ctx, querySQL)
	if err != nil {
		return lcs, err
	}
	defer rows.Close()

	for rows.Next() {
		var lc LogminerContent
		if err = rows.Scan(&lc.SCN, &lc.SegOwner, &lc.TableName, &lc.SQLRedo, &lc.SQLUndo, &lc.Operation); err != nil {
			return lcs, err
		}
		lcs = append(lcs, lc)
	}
	endTime := time.Now()
	zlog.Logger.Info("logminer sql",
		zap.String("sql", querySQL),
		zap.Time("end time", endTime),
		zap.String("cost time", time.Since(startTime).String()))
	return lcs, nil
}
