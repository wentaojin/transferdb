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
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/migrate/sql/oracle/public"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

func NewIncr(ctx context.Context, cfg *config.Config) (*Migrate, error) {
	oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}
	oracleMiner, err := oracle.NewOracleLogminerEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}
	mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
	if err != nil {
		return nil, err
	}
	metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MetaConfig, cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return nil, err
	}

	return &Migrate{
		Ctx:         ctx,
		Cfg:         cfg,
		Oracle:      oracleDB,
		OracleMiner: oracleMiner,
		Mysql:       mysqlDB,
		MetaDB:      metaDB,
	}, nil
}

func (r *Migrate) Incr() error {
	zap.L().Info("oracle to mysql increment sync table data start", zap.String("schema", r.Cfg.OracleConfig.SchemaName))

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := r.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if common.VersionOrdinal(oraDBVersion) < common.VersionOrdinal(common.RequireOracleDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oraDBVersion)
	}

	// 数据库字符集
	// AMERICAN_AMERICA.AL32UTF8
	charset, err := r.Oracle.GetOracleDBCharacterSet()
	if err != nil {
		return err
	}
	dbCharset := strings.Split(charset, ".")[1]
	if !strings.EqualFold(r.Cfg.OracleConfig.Charset, dbCharset) {
		return fmt.Errorf("oracle charset [%v] and oracle config charset [%v] aren't equal, please adjust oracle config charset", dbCharset, r.Cfg.OracleConfig.Charset)
	}

	// 获取配置文件待同步表列表
	exporters, err := public.FilterCFGTable(r.Cfg, r.Oracle)
	if err != nil {
		return err
	}

	// 判断 [wait_sync_meta] 是否存在错误记录，是否可进行 ALL
	errTotals, err := meta.NewWaitSyncMetaModel(r.MetaDB).CountsErrWaitSyncMetaBySchema(r.Ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
		TaskStatus:  common.TaskStatusFailed,
	})
	if errTotals > 0 || err != nil {
		return fmt.Errorf(`csv schema [%s] mode [%s] table task failed: %v, meta table [wait_sync_meta] exist failed error, please firstly check log and deal, secondly clear or update meta table [wait_sync_meta] column [task_status] table status WAITING (Need UPPER), thirdly clear meta table [full_sync_meta] error table record, fively clear target schema error table record, finally rerunning`, strings.ToUpper(r.Cfg.OracleConfig.SchemaName), r.Cfg.TaskMode, err)
	}

	// 全量数据导出导入，初始化全量元数据表以及导入完成初始化增量元数据表
	var (
		incrExistTableList, incrIsNotExistTableList []string
	)
	for _, tbl := range exporters {
		counts, err := meta.NewIncrSyncMetaModel(r.MetaDB).CountsIncrSyncMetaBySchemaTable(r.Ctx, &meta.IncrSyncMeta{
			DBTypeS:     r.Cfg.DBTypeS,
			DBTypeT:     r.Cfg.DBTypeT,
			SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
			TableNameS:  tbl,
		})
		if err != nil {
			return err
		}
		// 表不存在或者表数异常
		if counts == 0 || counts > 1 {
			incrIsNotExistTableList = append(incrIsNotExistTableList, tbl)
		}
		if counts == 1 {
			incrExistTableList = append(incrExistTableList, tbl)
		}
	}

	// 如果下游数据库增量元数据表 incr_sync_meta 存在迁移表记录
	if len(incrExistTableList) > 0 {
		// 配置文件获取表列表等于元数据库表列表，直接增量数据同步
		if len(incrExistTableList) == len(exporters) {
			// 根据 wait_sync_meta 数据记录判断表全量是否完成
			var panicTables []string
			for _, t := range exporters {
				waitSyncMetas, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMetaBySchemaTableSCN(r.Ctx, &meta.WaitSyncMeta{
					DBTypeS:     r.Cfg.DBTypeS,
					DBTypeT:     r.Cfg.DBTypeT,
					SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:  common.StringUPPER(t),
					TaskMode:    r.Cfg.TaskMode,
					TaskStatus:  common.TaskStatusSuccess,
				})
				if err != nil {
					return err
				}

				// 不存在表记录或者表记录超过多行
				if len(waitSyncMetas) == 0 || len(waitSyncMetas) > 1 {
					panicTables = append(panicTables, t)
				}

				// 存在表记录但是迁移总数与成功总数不相等
				if (len(waitSyncMetas) == 1) && (waitSyncMetas[0].ChunkTotalNums != waitSyncMetas[0].ChunkSuccessNums) {
					panicTables = append(panicTables, t)
				}
			}

			if len(panicTables) != 0 {
				return fmt.Errorf("table list %s can't incremently sync, because table increment sync meta record is exist and full meta sync isn't finished", panicTables)
			}
			// 增量数据同步
			for range time.Tick(300 * time.Millisecond) {
				if err := r.syncTableIncrRecord(); err != nil {
					return err
				}
			}
			return nil
		}

		// 配置文件获取的表列表不等于 increment_sync_meta 表列表数，不能直接增量同步，需要手工调整
		return fmt.Errorf("there is a migration table record for increment_sync_meta, but the configuration table list is not equal to the number of increment_sync_meta table lists, and it cannot be directly incrementally synchronized, please manually adjust to a list of meta-database tables [%v]", incrExistTableList)
	}

	// 如果下游数据库增量元数据表 incr_sync_meta 不存在任何记录，说明未进行过数据同步，则进行全量 + 增量数据同步
	if len(incrExistTableList) == 0 && len(incrIsNotExistTableList) == len(exporters) {
		// 全量同步
		err = r.Full()
		if err != nil {
			return err
		}

		// 全量任务结束，写入增量源数据表起始 SCN 号
		//根据配置文件生成同步表元数据 [incr_sync_meta]
		tableMetas, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMetaBySchema(r.Ctx, &meta.WaitSyncMeta{
			DBTypeS:     r.Cfg.DBTypeS,
			DBTypeT:     r.Cfg.DBTypeT,
			SchemaNameS: r.Cfg.OracleConfig.SchemaName,
			TaskMode:    r.Cfg.TaskMode,
			TaskStatus:  common.TaskStatusSuccess})
		if err != nil {
			return err
		}

		// 获取自定义库表名规则
		tableNameRule, err := r.GetTableNameRule()
		if err != nil {
			return err
		}

		var incrSyncMetas []meta.IncrSyncMeta
		if len(tableMetas) > 0 {
			for _, table := range tableMetas {
				// 库名、表名规则
				var targetTableName string
				if val, ok := tableNameRule[common.StringUPPER(table.TableNameS)]; ok {
					targetTableName = val
				} else {
					targetTableName = common.StringUPPER(table.TableNameS)
				}

				incrSyncMetas = append(incrSyncMetas, meta.IncrSyncMeta{
					DBTypeS:     r.Cfg.DBTypeS,
					DBTypeT:     r.Cfg.DBTypeT,
					GlobalScnS:  table.GlobalScnS,
					SchemaNameS: common.StringUPPER(table.SchemaNameS),
					TableNameS:  common.StringUPPER(table.TableNameS),
					SchemaNameT: common.StringUPPER(r.Cfg.MySQLConfig.SchemaName),
					TableNameT:  common.StringUPPER(targetTableName),
					TableScnS:   table.GlobalScnS,
					IsPartition: table.IsPartition,
				})
			}

			err = meta.NewIncrSyncMetaModel(r.MetaDB).BatchCreateIncrSyncMeta(
				r.Ctx, incrSyncMetas, r.Cfg.AppConfig.InsertBatchSize)
			if err != nil {
				return err
			}
		}

		// 增量数据同步
		for range time.Tick(300 * time.Millisecond) {
			if err = r.syncTableIncrRecord(); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("increment sync taskflow condition isn't match, can't sync")
}

func (r *Migrate) syncTableIncrRecord() error {
	// 获取自定义库表名规则
	tableNameRule, err := r.GetTableNameRule()
	if err != nil {
		return err
	}

	// 获取增量所需得日志文件
	logFiles, err := r.getTableIncrRecordLogfile()
	if err != nil {
		return err
	}
	zap.L().Info("increment table log file get",
		zap.String("logfile", fmt.Sprintf("%v", logFiles)))

	// 遍历所有日志文件
	for _, log := range logFiles {
		// 获取日志文件起始 SCN
		logFileStartSCN, err := common.StrconvUintBitSize(log["FIRST_CHANGE"], 64)
		if err != nil {
			return fmt.Errorf("get oracle log file start scn %s utils.StrconvUintBitSize failed: %v", log["FIRST_CHANGE"], err)
		}

		// 获取日志文件结束 SCN
		logFileEndSCN, err := common.StrconvUintBitSize(log["NEXT_CHANGE"], 64)
		if err != nil {
			return fmt.Errorf("get oracle log file end scn %s utils.StrconvUintBitSize failed: %v", log["NEXT_CHANGE"], err)
		}

		zap.L().Info("increment table log file logminer",
			zap.String("logfile", log["LOG_FILE"]),
			zap.Uint64("logfile start scn", logFileStartSCN),
			zap.Uint64("logminer start scn", logFileStartSCN),
			zap.Uint64("logfile end scn", logFileEndSCN))

		// 获取增量元数据表内所需同步表信息
		incrSyncMetas, err := meta.NewIncrSyncMetaModel(r.MetaDB).DetailIncrSyncMetaBySchema(r.Ctx, &meta.IncrSyncMeta{
			DBTypeS:     r.Cfg.DBTypeS,
			DBTypeT:     r.Cfg.DBTypeT,
			SchemaNameS: r.Cfg.OracleConfig.SchemaName,
		})
		if err != nil {
			return err
		}
		if len(incrSyncMetas) == 0 {
			return fmt.Errorf("mysql increment mete table [incr_sync_meta] can't null")
		}

		var (
			transferTableMetaMap map[string]uint64
			syncSourceTables     []string
		)
		transferTableMetaMap = make(map[string]uint64)
		for _, tbl := range incrSyncMetas {
			transferTableMetaMap[strings.ToUpper(tbl.TableNameS)] = tbl.TableScnS
			syncSourceTables = append(syncSourceTables, strings.ToUpper(tbl.TableNameS))
		}

		// 获取 logminer query 起始最小 SCN
		minSourceTableSCN, err := meta.NewIncrSyncMetaModel(r.MetaDB).GetIncrSyncMetaMinTableScnSBySchema(r.Ctx, &meta.IncrSyncMeta{
			DBTypeS:     r.Cfg.DBTypeS,
			DBTypeT:     r.Cfg.DBTypeT,
			SchemaNameS: r.Cfg.OracleConfig.SchemaName})
		if err != nil {
			return err
		}

		// logminer 运行
		if err = r.OracleMiner.AddOracleLogminerlogFile(log["LOG_FILE"]); err != nil {
			return err
		}

		if err = r.OracleMiner.StartOracleLogminerStoredProcedure(log["FIRST_CHANGE"]); err != nil {
			return err
		}

		// 捕获数据
		rowsResult, err := public.GetOracleIncrRecord(r.Ctx, r.OracleMiner,
			common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
			common.StringUPPER(r.Cfg.MySQLConfig.SchemaName),
			common.StringArrayToCapitalChar(syncSourceTables),
			tableNameRule,
			strconv.FormatUint(minSourceTableSCN, 10),
			r.Cfg.AllConfig.LogminerQueryTimeout)
		if err != nil {
			return err
		}
		zap.L().Info("increment table log extractor", zap.String("logfile", log["LOG_FILE"]),
			zap.Uint64("logfile start scn", logFileStartSCN),
			zap.Uint64("source table last scn", minSourceTableSCN),
			zap.Int("row counts", len(rowsResult)))

		// logminer 关闭
		if err = r.OracleMiner.EndOracleLogminerStoredProcedure(); err != nil {
			return err
		}

		// 获取 Oracle 所有 REDO 列表
		redoLogList, err := r.OracleMiner.GetOracleALLRedoLogFile()
		if err != nil {
			return err
		}

		//获取当前 CURRENT REDO LOG 信息
		currentRedoLogFirstChange, currentRedoLogMaxSCN, currentRedoLogFileName, err := r.OracleMiner.GetOracleCurrentRedoMaxSCN()
		if err != nil {
			return err
		}

		// 按表级别筛选数据
		var (
			logminerContentMap map[string][]public.Logminer
		)
		if len(rowsResult) > 0 {
			// 判断当前日志文件是否是重做日志文件
			if common.IsContainString(redoLogList, log["LOG_FILE"]) {
				// 判断是否是当前重做日志文件
				// 如果当前日志文件是当前重做日志文件则 FilterOracleIncrRecord 只运行一次大于或等于对应表数据记录，也就是只重放一次已消费得SCN
				if logFileStartSCN == currentRedoLogFirstChange && log["LOG_FILE"] == currentRedoLogFileName {
					logminerContentMap, err = public.FilterOracleIncrRecord(
						rowsResult,
						syncSourceTables,
						transferTableMetaMap,
						r.Cfg.AllConfig.FilterThreads,
						common.MigrateCurrentResetFlag,
					)
					if err != nil {
						return err
					}
					zap.L().Warn("oracle current redo log reset flag", zap.Int("MigrateCurrentResetFlag", common.MigrateCurrentResetFlag))
					common.MigrateCurrentResetFlag = 1
				} else {
					logminerContentMap, err = public.FilterOracleIncrRecord(
						rowsResult,
						syncSourceTables,
						transferTableMetaMap,
						r.Cfg.AllConfig.FilterThreads,
						0,
					)
					if err != nil {
						return err
					}
				}

				if len(logminerContentMap) > 0 {
					// 数据应用
					if err := applyOracleIncrRecord(r.MetaDB, r.Mysql, r.Cfg, logminerContentMap); err != nil {
						return err
					}
					if logFileStartSCN == currentRedoLogFirstChange && log["LOG_FILE"] == currentRedoLogFileName {
						// 当前所有日志文件内容应用完毕，判断是否直接更新 GLOBAL_SCN 至当前重做日志文件起始 SCN
						err = meta.NewCommonModel(r.MetaDB).UpdateIncrSyncMetaSCNByCurrentRedo(r.Ctx,
							r.Cfg.DBTypeS,
							r.Cfg.DBTypeT,
							r.Cfg.OracleConfig.SchemaName,
							currentRedoLogMaxSCN,
							logFileStartSCN,
							logFileEndSCN)
						if err != nil {
							return err
						}
					} else {
						// 当前所有日志文件内容应用完毕，直接更新 GLOBAL_SCN 至日志文件结束 SCN
						err = meta.NewCommonModel(r.MetaDB).UpdateIncrSyncMetaSCNByNonCurrentRedo(r.Ctx,
							r.Cfg.DBTypeS,
							r.Cfg.DBTypeT,
							r.Cfg.OracleConfig.SchemaName,
							currentRedoLogMaxSCN,
							logFileStartSCN,
							logFileEndSCN,
							syncSourceTables)
						if err != nil {
							return err
						}
					}

					continue
				}
				zap.L().Warn("increment table log file logminer data that needn't to be consumed by current redo, transferdb will continue to capture")
				continue
			}
			logminerContentMap, err = public.FilterOracleIncrRecord(
				rowsResult,
				syncSourceTables,
				transferTableMetaMap,
				r.Cfg.AllConfig.FilterThreads,
				0,
			)
			if err != nil {
				return err
			}
			if len(logminerContentMap) > 0 {
				// 数据应用
				if err := applyOracleIncrRecord(r.MetaDB, r.Mysql, r.Cfg, logminerContentMap); err != nil {
					return err
				}
				// 当前所有日志文件内容应用完毕，直接更新 GLOBAL_SCN 至日志文件结束 SCN
				err = meta.NewCommonModel(r.MetaDB).UpdateIncrSyncMetaSCNByArchivedLog(r.Ctx,
					r.Cfg.DBTypeS,
					r.Cfg.DBTypeT,
					r.Cfg.OracleConfig.SchemaName,
					logFileEndSCN,
					syncSourceTables)
				if err != nil {
					return err
				}
				continue
			}
			zap.L().Warn("increment table log file logminer data that needn't to be consumed by logfile, transferdb will continue to capture")
			continue
		}

		// 当前日志文件不存在数据记录
		if common.IsContainString(redoLogList, log["LOG_FILE"]) {
			if logFileStartSCN == currentRedoLogFirstChange && log["LOG_FILE"] == currentRedoLogFileName {
				// 当前所有日志文件内容应用完毕，判断是否直接更新 GLOBAL_SCN 至当前重做日志文件起始 SCN
				err = meta.NewCommonModel(r.MetaDB).UpdateIncrSyncMetaSCNByCurrentRedo(r.Ctx,
					r.Cfg.DBTypeS,
					r.Cfg.DBTypeT,
					r.Cfg.OracleConfig.SchemaName,
					currentRedoLogMaxSCN,
					logFileStartSCN,
					logFileEndSCN)
				if err != nil {
					return err
				}
			} else {
				// 当前所有日志文件内容应用完毕，判断是否更新 GLOBAL_SCN 至日志文件结束 SCN
				err = meta.NewCommonModel(r.MetaDB).UpdateIncrSyncMetaSCNByNonCurrentRedo(r.Ctx,
					r.Cfg.DBTypeS,
					r.Cfg.DBTypeT,
					r.Cfg.OracleConfig.SchemaName,
					currentRedoLogMaxSCN,
					logFileStartSCN,
					logFileEndSCN,
					syncSourceTables)
				if err != nil {
					return err
				}
			}
		} else {
			// 当前所有日志文件内容应用完毕，直接更新 GLOBAL_SCN 至日志文件结束 SCN
			err = meta.NewCommonModel(r.MetaDB).UpdateIncrSyncMetaSCNByArchivedLog(r.Ctx,
				r.Cfg.DBTypeS,
				r.Cfg.DBTypeT,
				r.Cfg.OracleConfig.SchemaName,
				logFileEndSCN,
				syncSourceTables)
			if err != nil {
				return err
			}
		}
		zap.L().Warn("increment table log file logminer null data, transferdb will continue to capture")
		continue
	}
	return nil
}

func (r *Migrate) getTableIncrRecordLogfile() ([]map[string]string, error) {
	var logFiles []map[string]string

	// 获取增量表起始最小 SCN 号
	globalSCN, err := meta.NewIncrSyncMetaModel(r.MetaDB).GetIncrSyncMetaMinGlobalScnSBySchema(r.Ctx, &meta.IncrSyncMeta{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: r.Cfg.OracleConfig.SchemaName,
	})
	if err != nil {
		return logFiles, err
	}
	strGlobalSCN := strconv.FormatUint(globalSCN, 10)

	// 判断数据是在 archived log Or redo log
	// 如果 redoSCN 等于 0，说明数据在归档日志
	redoScn, err := r.OracleMiner.GetOracleRedoLogSCN(strGlobalSCN)
	if err != nil {
		return logFiles, err
	}

	archivedScn, err := r.OracleMiner.GetOracleArchivedLogSCN(strGlobalSCN)
	if err != nil {
		return logFiles, err
	}

	// 获取所需挖掘的日志文件
	if redoScn == 0 {
		strArchivedSCN := strconv.FormatUint(archivedScn, 10)
		logFiles, err = r.OracleMiner.GetOracleArchivedLogFile(strArchivedSCN)
		if err != nil {
			return logFiles, err
		}
	} else {
		strRedoCN := strconv.FormatUint(redoScn, 10)
		logFiles, err = r.OracleMiner.GetOracleRedoLogFile(strRedoCN)
		if err != nil {
			return logFiles, err
		}
	}
	return logFiles, nil
}
