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
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
)

func (o *Oracle) GetOracleRedoLogSCN(scn string) (uint64, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`select  FIRST_CHANGE# AS SCN
  from (SELECT  GROUP#,
               first_change#,
               MEMBERS,
               FIRST_TIME
          FROM v$LOG
         WHERE FIRST_CHANGE# <= `, scn, ` order by FIRST_CHANGE# desc)
 where rownum = 1`))
	var globalSCN uint64
	if err != nil {
		return globalSCN, err
	}
	if len(res) > 0 {
		globalSCN, err = common.StrconvUintBitSize(res[0]["SCN"], 64)
		if err != nil {
			return globalSCN, fmt.Errorf("get oracle redo log scn %s utils.StrconvUintBitSize failed: %v", res[0]["SCN"], err)
		}
	}
	return globalSCN, nil
}

func (o *Oracle) GetOracleArchivedLogSCN(scn string) (uint64, error) {
	var globalSCN uint64
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`select FIRST_CHANGE# AS SCN
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
		globalSCN, err = common.StrconvUintBitSize(res[0]["SCN"], 64)
		if err != nil {
			return globalSCN, fmt.Errorf("get oracle archive log scn %s utils.StrconvUintBitSize failed: %v", res[0]["SCN"], err)
		}
	}
	return globalSCN, nil
}

func (o *Oracle) GetOracleRedoLogFile(scn string) ([]map[string]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT 
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

func (o *Oracle) GetOracleArchivedLogFile(scn string) ([]map[string]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT NAME AS LOG_FILE,
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

func (o *Oracle) GetOracleCurrentRedoMaxSCN() (uint64, uint64, string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT
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
	firstSCN, err := common.StrconvUintBitSize(res[0]["FIRST_CHANGE"], 64)
	if err != nil {
		return firstSCN, 0, res[0]["LOG_FILE"], fmt.Errorf("get oracle current redo first_change scn %s utils.StrconvUintBitSize falied: %v", res[0]["FIRST_CHANGE"], err)
	}
	maxSCN, err := common.StrconvUintBitSize(res[0]["NEXT_CHANGE"], 64)
	if err != nil {
		return firstSCN, maxSCN, res[0]["LOG_FILE"], fmt.Errorf("get oracle current redo next_change scn %s utils.StrconvUintBitSize falied: %v", res[0]["NEXT_CHANGE"], err)
	}
	if maxSCN == 0 || firstSCN == 0 {
		return firstSCN, maxSCN, res[0]["LOG_FILE"], fmt.Errorf("GetOracleCurrentRedoMaxSCN value is euqal to 0, does't meet expectations")
	}
	return firstSCN, maxSCN, res[0]["LOG_FILE"], nil
}

func (o *Oracle) GetOracleALLRedoLogFile() ([]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT
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

func (o *Oracle) AddOracleLogminerlogFile(logFile string) error {
	ctx, _ := context.WithCancel(context.Background())
	sql := common.StringsBuilder(`BEGIN
  dbms_logmnr.add_logfile(logfilename => '`, logFile, `',
                          options     => dbms_logmnr.NEW);
END;`)
	_, err := o.OracleDB.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("oracle logminer sql [%v] add log file [%s] failed: %v", sql, logFile, err)
	}
	return nil
}

func (o *Oracle) StartOracleLogminerStoredProcedure(scn string) error {
	ctx, _ := context.WithCancel(context.Background())
	sql := common.StringsBuilder(`BEGIN
  dbms_logmnr.start_logmnr(startSCN => `, scn, `,
                           options  => SYS.DBMS_LOGMNR.SKIP_CORRUPTION +       -- 日志遇到坏块，不报错退出，直接跳过
                                       SYS.DBMS_LOGMNR.NO_SQL_DELIMITER +
                                       SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT +
                                       SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                                       SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                       SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);
END;`)
	_, err := o.OracleDB.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("oracle logminer stored procedure sql [%v] startscn [%v] failed: %v", sql, scn, err)
	}
	return nil
}

func (o *Oracle) EndOracleLogminerStoredProcedure() error {
	ctx, _ := context.WithCancel(context.Background())
	_, err := o.OracleDB.ExecContext(ctx, common.StringsBuilder(`BEGIN
  dbms_logmnr.end_logmnr();
END;`))
	if err != nil {
		return fmt.Errorf("oracle logminer stored procedure end failed: %v", err)
	}
	return nil
}
