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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/transferdb/utils"

	"go.uber.org/zap"
)

func (e *Engine) GetOracleRedoLogSCN(scn string) (int, error) {
	_, res, err := Query(e.OracleDB, utils.StringsBuilder(`select  FIRST_CHANGE# AS SCN
  from (SELECT  GROUP#,
               first_change#,
               MEMBERS,
               FIRST_TIME
          FROM v$LOG
         WHERE FIRST_CHANGE# <= `, scn, ` order by FIRST_CHANGE# desc)
 where rownum = 1`))
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

func (e *Engine) GetOracleArchivedLogSCN(scn string) (int, error) {
	var globalSCN int
	_, res, err := Query(e.OracleDB, utils.StringsBuilder(`select FIRST_CHANGE# AS SCN
  from (select FIRST_CHANGE#,FIRST_TIME
          from v$archived_log
         where STATUS = 'A'
           and DELETED='NO'
           and FIRST_CHANGE# <= `, scn, ` order by FIRST_CHANGE# desc)
 where rownum = 1`))
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

func (e *Engine) GetOracleRedoLogFile(scn string) ([]map[string]string, error) {
	_, res, err := Query(e.OracleDB, utils.StringsBuilder(`SELECT 
       --l.GROUP# GROUP_NUMBER,
       l.FIRST_CHANGE# AS FIRST_CHANGE,
       --l.BYTES / 1024 / 1024 AS LOG_SIZE,
       L.NEXT_CHANGE# AS NEXT_CHANGE,
       lf.MEMBER LOG_FILE
  FROM v$LOGFILE lf, v$LOG l
 WHERE l.GROUP# = lf.GROUP#
   AND l.FIRST_CHANGE# >= `, scn, ` ORDER BY l.FIRST_CHANGE# ASC`))
	if err != nil {
		return []map[string]string{}, err
	}
	return res, nil
}

func (e *Engine) GetOracleArchivedLogFile(scn string) ([]map[string]string, error) {
	_, res, err := Query(e.OracleDB, utils.StringsBuilder(`SELECT NAME AS LOG_FILE,
       NEXT_CHANGE# AS NEXT_CHANGE,
       --BLOCKS * BLOCK_SIZE / 1024 / 1024 AS LOG_SIZE,
       FIRST_CHANGE# AS FIRST_CHANGE
  FROM v$ARCHIVED_LOG
 WHERE STATUS = 'A'
   AND DELETED = 'NO'
   AND FIRST_CHANGE# >= `, scn, ` ORDER BY FIRST_CHANGE# ASC`))
	if err != nil {
		return []map[string]string{}, err
	}
	return res, nil
}

func (e *Engine) GetOracleCurrentRedoMaxSCN() (int, int, string, error) {
	_, res, err := Query(e.OracleDB, utils.StringsBuilder(`SELECT
       l.FIRST_CHANGE# AS FIRST_CHANGE,
       l.NEXT_CHANGE# AS NEXT_CHANGE,
       lf.MEMBER LOG_FILE
  FROM v$LOGFILE lf, v$LOG l
 WHERE l.GROUP# = lf.GROUP#
 AND l.STATUS='CURRENT'`))
	if err != nil {
		return 0, 0, "", err
	}
	if len(res) == 0 {
		return 0, 0, "", fmt.Errorf("oracle current redo log can't null")
	}
	firstSCN, err := strconv.Atoi(res[0]["FIRST_CHANGE"])
	if err != nil {
		return firstSCN, 0, res[0]["LOG_FILE"], fmt.Errorf("GetOracleCurrentRedoMaxSCN strconv.Atoi falied: %v", err)
	}
	maxSCN, err := strconv.Atoi(res[0]["NEXT_CHANGE"])
	if err != nil {
		return firstSCN, maxSCN, res[0]["LOG_FILE"], fmt.Errorf("GetOracleCurrentRedoMaxSCN strconv.Atoi falied: %v", err)
	}
	if maxSCN == 0 || firstSCN == 0 {
		return firstSCN, maxSCN, res[0]["LOG_FILE"], fmt.Errorf("GetOracleCurrentRedoMaxSCN value is euqal to 0, does't meet expectations")
	}
	return firstSCN, maxSCN, res[0]["LOG_FILE"], nil
}

func (e *Engine) GetOracleALLRedoLogFile() ([]string, error) {
	_, res, err := Query(e.OracleDB, utils.StringsBuilder(`SELECT
       lf.MEMBER LOG_FILE
  FROM v$LOGFILE lf`))
	if err != nil {
		return []string{}, err
	}
	if len(res) == 0 {
		return []string{}, fmt.Errorf("oracle all redo log can't null")
	}

	var logs []string
	for _, r := range res {
		logs = append(logs, r["LOG_FILE"])
	}
	return logs, nil
}

func (e *Engine) GetMySQLTableIncrementMetaMinGlobalSCNTime(sourceSchemaName string) (int, error) {
	var globalSCN int
	if err := e.GormDB.Model(&IncrementSyncMeta{}).Where("source_schema_name = ?",
		strings.ToUpper(sourceSchemaName)).
		Distinct().
		Order("global_scn ASC").Limit(1).Pluck("global_scn", &globalSCN).Error; err != nil {
		return globalSCN, err
	}
	return globalSCN, nil
}

func (e *Engine) GetMySQLTableIncrementMetaMinSourceTableSCNTime(sourceSchemaName string) (int, error) {
	var sourceTableSCN int
	if err := e.GormDB.Model(&IncrementSyncMeta{}).Where("source_schema_name = ?",
		strings.ToUpper(sourceSchemaName)).
		Distinct().
		Order("source_table_scn ASC").Limit(1).Pluck("source_table_scn", &sourceTableSCN).Error; err != nil {
		return sourceTableSCN, err
	}
	return sourceTableSCN, nil
}

func (e *Engine) GetMySQLTableIncrementMetaRecord(sourceSchemaName string) ([]string, map[string]int, error) {
	var (
		incrementMeta      []IncrementSyncMeta
		tableMetas         map[string]int
		transferTableSlice []string
	)
	if err := e.GormDB.Where("source_schema_name = ?", strings.ToUpper(sourceSchemaName)).
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
	ctx, _ := context.WithCancel(context.Background())
	_, err := e.OracleDB.ExecContext(ctx, utils.StringsBuilder(`BEGIN
  dbms_logmnr.add_logfile(logfilename => '`, logFile, `',
                          options     => dbms_logmnr.NEW);
END;`))
	if err != nil {
		return fmt.Errorf("oracle logminer add log file failed: %v", err)
	}
	return nil
}

func (e *Engine) StartOracleLogminerStoredProcedure(scn string) error {
	ctx, _ := context.WithCancel(context.Background())
	_, err := e.OracleDB.ExecContext(ctx, utils.StringsBuilder(`BEGIN
  dbms_logmnr.start_logmnr(startSCN => `, scn, `,
                           options  => SYS.DBMS_LOGMNR.SKIP_CORRUPTION +       -- 日志遇到坏块，不报错退出，直接跳过
                                       SYS.DBMS_LOGMNR.NO_SQL_DELIMITER +
                                       SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT +
                                       SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                                       SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                       SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);
END;`))
	if err != nil {
		return fmt.Errorf("oracle logminer stored procedure start failed: %v", err)
	}
	return nil
}

func (e *Engine) EndOracleLogminerStoredProcedure() error {
	ctx, _ := context.WithCancel(context.Background())
	_, err := e.OracleDB.ExecContext(ctx, utils.StringsBuilder(`BEGIN
  dbms_logmnr.end_logmnr();
END;`))
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

func (e *Engine) GetOracleLogminerContentToMySQL(schemaName string, sourceTableNameList string, lastCheckpoint string, logminerQueryTimeout int) ([]LogminerContent, error) {
	var lcs []LogminerContent

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(logminerQueryTimeout)*time.Second)
	defer cancel()

	querySQL := utils.StringsBuilder(`SELECT SCN,
       SEG_OWNER,
       TABLE_NAME,
       SQL_REDO,
       SQL_UNDO,
       OPERATION
  FROM V$LOGMNR_CONTENTS
 WHERE 1 = 1
   AND UPPER(SEG_OWNER) = '`, strings.ToUpper(schemaName), `'
   AND UPPER(TABLE_NAME) IN (`, sourceTableNameList, `)
   AND OPERATION IN ('INSERT', 'DELETE', 'UPDATE', 'DDL')
   AND SCN >= `, lastCheckpoint, ` ORDER BY SCN`)

	startTime := time.Now()
	Logger.Info("logminer sql",
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
	Logger.Info("logminer sql",
		zap.String("sql", querySQL),
		zap.Time("end time", endTime),
		zap.String("cost time", time.Since(startTime).String()))
	return lcs, nil
}
