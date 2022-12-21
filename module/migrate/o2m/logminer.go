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
package o2m

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"
)

// 获取 Oracle logminer 日志内容并过滤筛选已提交的 INSERT/DELETE/UPDATE 事务语句
// 考虑异构数据库，只同步 INSERT/DELETE/UPDATE 事务语句以及 TRUNCATE TABLE/DROP TABLE DDL 语句，其他类型 SQL 不同步
// V$LOGMNR_CONTENTS 字段解释参考链接
// https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-LOGMNR_CONTENTS.html#GUID-B9196942-07BF-4935-B603-FA875064F5C3
type logminer struct {
	SCN       uint64
	SegOwner  string
	TableName string
	SQLRedo   string
	SQLUndo   string
	Operation string
}

// 捕获增量数据
func getOracleIncrRecord(ctx context.Context, oracle *oracle.Oracle, sourceSchema string, sourceTable string, lastCheckpoint string, queryTimeout int) ([]logminer, error) {
	var lcs []logminer

	c, cancel := context.WithTimeout(ctx, time.Duration(queryTimeout)*time.Second)
	defer cancel()

	querySQL := common.StringsBuilder(`SELECT SCN,
       SEG_OWNER,
       TABLE_NAME,
       SQL_REDO,
       SQL_UNDO,
       OPERATION
  FROM V$LOGMNR_CONTENTS
 WHERE 1 = 1
   AND UPPER(SEG_OWNER) = '`, common.StringUPPER(sourceSchema), `'
   AND UPPER(TABLE_NAME) IN (`, sourceTable, `)
   AND OPERATION IN ('INSERT', 'DELETE', 'UPDATE', 'DDL')
   AND SCN >= `, lastCheckpoint, ` ORDER BY SCN`)

	startTime := time.Now()

	rows, err := oracle.OracleDB.QueryContext(c, querySQL)
	if err != nil {
		return lcs, err
	}
	defer rows.Close()

	for rows.Next() {
		var lc logminer
		if err = rows.Scan(&lc.SCN, &lc.SegOwner, &lc.TableName, &lc.SQLRedo, &lc.SQLUndo, &lc.Operation); err != nil {
			return lcs, err
		}
		lcs = append(lcs, lc)
	}
	endTime := time.Now()

	jsonLCS, err := json.Marshal(lcs)
	if err != nil {
		return lcs, fmt.Errorf("json Marshal logminer failed: %v", err)
	}
	zap.L().Info("logminer sql",
		zap.String("sql", querySQL),
		zap.String("json logminer content", string(jsonLCS)),
		zap.String("start time", startTime.String()),
		zap.String("end time", endTime.String()),
		zap.String("cost time", endTime.Sub(startTime).String()))
	return lcs, nil
}

// 按表级别筛选以及过滤数据
func filterOracleIncrRecord(
	lognimers []logminer,
	exporters []string,
	exporterTableSourceSCN map[string]uint64,
	workerThreads, currentResetFlag int) (map[string][]logminer, error) {
	var (
		lcMap map[string][]logminer
		lc    []logminer
	)
	lcMap = make(map[string][]logminer)

	for _, table := range exporters {
		lcMap[common.StringUPPER(table)] = lc
	}

	startTime := time.Now()
	zap.L().Info("oracle table redo filter start",
		zap.Time("start time", startTime))

	c := make(chan struct{})

	// 开始准备从 channel 接收数据了
	s := NewScheduleJob(workerThreads, lcMap, func() { c <- struct{}{} })

	g := &errgroup.Group{}
	g.SetLimit(workerThreads)

	for _, rs := range lognimers {
		sourceTableSCNMAP := exporterTableSourceSCN
		rows := rs
		g.Go(func() error {
			// 筛选过滤 Oracle Redo SQL
			// 1、数据同步只同步 INSERT/DELETE/UPDATE DML以及只同步 truncate table/ drop table 限定 DDL
			// 2、根据元数据表 incr_synce_meta 对应表已经同步写入得 SCN SQL 记录,过滤 Oracle 提交记录 SCN 号，过滤,防止重复写入
			if currentResetFlag == 0 {
				if rows.SCN >= sourceTableSCNMAP[strings.ToUpper(rows.TableName)] {
					if rows.Operation == common.MigrateOperationDDL {
						splitDDL := strings.Split(rows.SQLRedo, ` `)
						ddl := common.StringsBuilder(splitDDL[0], ` `, splitDDL[1])
						if strings.ToUpper(ddl) == common.MigrateOperationDropTable {
							// 处理 drop table marvin8 AS "BIN$vVWfliIh6WfgU0EEEKzOvg==$0"
							rows.SQLRedo = strings.Split(strings.ToUpper(rows.SQLRedo), "AS")[0]
							s.AddData(rows)
						}
						if strings.ToUpper(ddl) == common.MigrateOperationTruncateTable {
							// 处理 truncate table marvin8
							s.AddData(rows)
						}
					} else {
						s.AddData(rows)
					}
				}
				return nil

			} else if currentResetFlag == 1 {
				if rows.SCN > sourceTableSCNMAP[strings.ToUpper(rows.TableName)] {
					if rows.Operation == common.MigrateOperationDDL {
						splitDDL := strings.Split(rows.SQLRedo, ` `)
						ddl := common.StringsBuilder(splitDDL[0], ` `, splitDDL[1])
						if strings.ToUpper(ddl) == common.MigrateOperationDropTable {
							// 处理 drop table marvin8 AS "BIN$vVWfliIh6WfgU0EEEKzOvg==$0"
							rows.SQLRedo = strings.Split(strings.ToUpper(rows.SQLRedo), "AS")[0]
							s.AddData(rows)
						}
						if strings.ToUpper(ddl) == common.MigrateOperationTruncateTable {
							// 处理 truncate table marvin8
							s.AddData(rows)
						}
					} else {
						s.AddData(rows)
					}
				}
				return nil
			} else {
				return fmt.Errorf("filterOracleIncrRecord meet error, isFirstRun value error")
			}
		})
	}
	if err := g.Wait(); err != nil {
		return lcMap, fmt.Errorf("filter oracle redo record by table error: %v", err)
	}

	s.Close()
	<-c

	endTime := time.Now()
	zap.L().Info("oracle table filter finished",
		zap.String("status", "success"),
		zap.Time("start time", startTime),
		zap.Time("end time", endTime),
		zap.String("cost time", time.Since(startTime).String()))

	return lcMap, nil
}
