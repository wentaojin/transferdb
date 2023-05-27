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
package o2t

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
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"time"
)

type Migrate struct {
	Ctx         context.Context
	Cfg         *config.Config
	Oracle      *oracle.Oracle
	OracleMiner *oracle.Oracle
	Mysql       *mysql.MySQL
	MetaDB      *meta.Meta
}

func NewFuller(ctx context.Context, cfg *config.Config) (*Migrate, error) {
	oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
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
		Ctx:    ctx,
		Cfg:    cfg,
		Oracle: oracleDB,
		Mysql:  mysqlDB,
		MetaDB: metaDB,
	}, nil
}

func (r *Migrate) Full() error {
	startTime := time.Now()
	zap.L().Info("source schema full table data sync start",
		zap.String("schema", r.Cfg.OracleConfig.SchemaName))

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oracleDBVersion, err := r.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if common.VersionOrdinal(oracleDBVersion) < common.VersionOrdinal(common.RequireOracleDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oracleDBVersion)
	}
	oracleCollation := false
	if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oracleCollation = true
	}

	// 数据库字符集
	// AMERICAN_AMERICA.AL32UTF8
	charset, err := r.Oracle.GetOracleDBCharacterSet()
	if err != nil {
		return err
	}
	dbCharset := strings.Split(charset, ".")[1]
	if !strings.EqualFold(r.Cfg.OracleConfig.Charset, dbCharset) {
		zap.L().Warn("oracle charset and oracle config charset",
			zap.String("oracle charset", dbCharset),
			zap.String("oracle config charset", r.Cfg.OracleConfig.Charset))
	}

	// 获取配置文件待同步表列表
	exporters, err := public.FilterCFGTable(r.Cfg, r.Oracle)
	if err != nil {
		return err
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [wait_sync_meta],重新运行全量任务
	if !r.Cfg.FullConfig.EnableCheckpoint {
		err = meta.NewFullSyncMetaModel(r.MetaDB).DeleteFullSyncMetaBySchemaSyncMode(
			r.Ctx, &meta.FullSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: r.Cfg.OracleConfig.SchemaName,
				TaskMode:    common.StringUPPER(r.Cfg.TaskMode),
			})
		if err != nil {
			return err
		}

		err = meta.NewChunkErrorDetailModel(r.MetaDB).DeleteChunkErrorDetailBySchemaTaskMode(r.Ctx, &meta.ChunkErrorDetail{
			DBTypeS:     r.Cfg.DBTypeS,
			DBTypeT:     r.Cfg.DBTypeT,
			SchemaNameS: r.Cfg.OracleConfig.SchemaName,
			TaskMode:    r.Cfg.TaskMode,
		})
		if err != nil {
			return err
		}

		for _, tableName := range exporters {
			err = meta.NewWaitSyncMetaModel(r.MetaDB).DeleteWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: r.Cfg.OracleConfig.SchemaName,
				TableNameS:  tableName,
				TaskMode:    r.Cfg.TaskMode,
			})
			if err != nil {
				return err
			}
			// 清理已有表数据
			if err := r.Mysql.TruncateMySQLTable(r.Cfg.MySQLConfig.SchemaName, tableName); err != nil {
				return err
			}
			zap.L().Info("truncate table",
				zap.String("schema", r.Cfg.MySQLConfig.SchemaName),
				zap.String("table", tableName),
				zap.String("status", "success"))

			// 判断并记录待同步表列表
			waitSyncMetas, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
				TableNameS:  tableName,
				TaskMode:    r.Cfg.TaskMode,
			})
			if err != nil {
				return err
			}
			if len(waitSyncMetas) == 0 {
				err = meta.NewWaitSyncMetaModel(r.MetaDB).CreateWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
					DBTypeS:        r.Cfg.DBTypeS,
					DBTypeT:        r.Cfg.DBTypeT,
					SchemaNameS:    common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:     common.StringUPPER(tableName),
					TaskMode:       r.Cfg.TaskMode,
					TaskStatus:     common.TaskStatusWaiting,
					GlobalScnS:     common.TaskTableDefaultSourceGlobalSCN,
					ChunkTotalNums: common.TaskTableDefaultSplitChunkNums,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	// 清理非当前任务 SUCCESS 表元数据记录 wait_sync_meta (用于统计 SUCCESS 准备)
	// 例如：当前任务表 A/B，之前任务表 A/C (SUCCESS)，清理元数据 C，对于表 A 任务 Skip 忽略处理，除非手工清理表 A
	tablesByMeta, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMetaSuccessTables(r.Ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
		TaskStatus:  common.TaskStatusSuccess,
	})
	if err != nil {
		return err
	}

	clearTables := common.FilterDifferenceStringItems(tablesByMeta, exporters)
	interTables := common.FilterIntersectionStringItems(tablesByMeta, exporters)
	if len(clearTables) > 0 {
		err = meta.NewWaitSyncMetaModel(r.MetaDB).DeleteWaitSyncMetaSuccessTables(r.Ctx, &meta.WaitSyncMeta{
			DBTypeS:     r.Cfg.DBTypeS,
			DBTypeT:     r.Cfg.DBTypeT,
			SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
			TaskMode:    r.Cfg.TaskMode,
			TaskStatus:  common.TaskStatusSuccess,
		}, clearTables)
		if err != nil {
			return err
		}
	}
	zap.L().Warn("non-task table clear",
		zap.Strings("clear tables", clearTables),
		zap.Strings("intersection tables", interTables),
		zap.Int("clear totals", len(clearTables)),
		zap.Int("intersection total", len(interTables)))

	// 判断 [wait_sync_meta] 是否存在错误记录，是否可进行 FULL
	errTotals, err := meta.NewWaitSyncMetaModel(r.MetaDB).CountsErrWaitSyncMetaBySchema(r.Ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
		TaskStatus:  common.TaskStatusFailed,
	})
	if err != nil {
		return err
	}
	if errTotals > 0 {
		return fmt.Errorf(`full schema [%s] mode [%s] table task failed: meta table [wait_sync_meta] exist failed error, please: firstly check meta table [wait_sync_meta] and [full_sync_meta] log record; secondly if need resume, update meta table [wait_sync_meta] column [task_status] table status RUNNING (Need UPPER) and delete meta table [chunk_error_detail] current task all records; finally rerunning`, strings.ToUpper(r.Cfg.OracleConfig.SchemaName), r.Cfg.TaskMode)
	}

	// 判断并记录待同步表列表
	for _, tableName := range exporters {
		waitSyncMetas, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
			DBTypeS:     r.Cfg.DBTypeS,
			DBTypeT:     r.Cfg.DBTypeT,
			SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
			TableNameS:  tableName,
			TaskMode:    r.Cfg.TaskMode,
		})
		if err != nil {
			return err
		}
		if len(waitSyncMetas) == 0 {
			err = meta.NewWaitSyncMetaModel(r.MetaDB).CreateWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
				DBTypeS:        r.Cfg.DBTypeS,
				DBTypeT:        r.Cfg.DBTypeT,
				SchemaNameS:    common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
				TableNameS:     common.StringUPPER(tableName),
				TaskMode:       r.Cfg.TaskMode,
				TaskStatus:     common.TaskStatusWaiting,
				GlobalScnS:     common.TaskTableDefaultSourceGlobalSCN,
				ChunkTotalNums: common.TaskTableDefaultSplitChunkNums,
			})
			if err != nil {
				return err
			}
		}
	}

	// 获取等待同步以及未同步完成的表列表
	var (
		waitSyncTableMetas []meta.WaitSyncMeta
		waitSyncTables     []string
	)

	waitSyncDetails, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
		DBTypeS:        r.Cfg.DBTypeS,
		DBTypeT:        r.Cfg.DBTypeT,
		SchemaNameS:    common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:       r.Cfg.TaskMode,
		TaskStatus:     common.TaskStatusWaiting,
		GlobalScnS:     common.TaskTableDefaultSourceGlobalSCN,
		ChunkTotalNums: common.TaskTableDefaultSplitChunkNums,
	})
	if err != nil {
		return err
	}
	waitSyncTableMetas = waitSyncDetails
	if len(waitSyncTableMetas) > 0 {
		for _, table := range waitSyncTableMetas {
			waitSyncTables = append(waitSyncTables, common.StringUPPER(table.TableNameS))
		}
	}

	// 判断未同步完成的表能否断点续传
	var (
		partSyncTables    []string
		panicTblFullSlice []string
	)
	partSyncDetails, err := meta.NewWaitSyncMetaModel(r.MetaDB).QueryWaitSyncMetaByPartTask(r.Ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
		TaskStatus:  common.TaskStatusRunning,
	})
	if err != nil {
		return err
	}
	if len(partSyncDetails) > 0 {
		for _, t := range partSyncDetails {
			// 判断 running 状态表 chunk 数是否一致，一致可断点续传
			chunkCounts, err := meta.NewFullSyncMetaModel(r.MetaDB).CountsFullSyncMetaByTaskTable(r.Ctx, &meta.FullSyncMeta{
				DBTypeS:     t.DBTypeS,
				DBTypeT:     t.DBTypeT,
				SchemaNameS: common.StringUPPER(t.SchemaNameS),
				TableNameS:  t.TableNameS,
				TaskMode:    t.TaskMode,
			})
			if err != nil {
				return err
			}
			if chunkCounts != t.ChunkTotalNums {
				panicTblFullSlice = append(panicTblFullSlice, t.TableNameS)
			} else {
				partSyncTables = append(partSyncTables, t.TableNameS)
			}
		}
	}

	if len(panicTblFullSlice) > 0 {
		endTime := time.Now()
		zap.L().Error("all oracle table data full error",
			zap.String("schema", r.Cfg.OracleConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Int("part sync tables", len(partSyncTables)),
			zap.Strings("panic tables", panicTblFullSlice))
		return fmt.Errorf("checkpoint isn't consistent, can't be resume, please reruning [enable-checkpoint = fase]")
	}

	// 数据迁移
	// 优先存在断点的表
	// partSyncTables -> waitSyncTables
	if len(partSyncTables) > 0 {
		err = r.FullPartSyncTable(partSyncTables)
		if err != nil {
			return err
		}
	}
	if len(waitSyncTables) > 0 {
		err = r.FullWaitSyncTable(waitSyncTables, oracleCollation)
		if err != nil {
			return err
		}
	}

	// 任务详情
	succTotals, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
		TaskStatus:  common.TaskStatusSuccess,
	})
	if err != nil {
		return err
	}
	failedTotals, err := meta.NewWaitSyncMetaModel(r.MetaDB).DetailWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
		TaskStatus:  common.TaskStatusFailed,
	})
	if err != nil {
		return err
	}

	zap.L().Info("all full table data sync finished",
		zap.String("schema", r.Cfg.OracleConfig.SchemaName),
		zap.Int("table totals", len(exporters)),
		zap.Int("table success", len(succTotals)),
		zap.Int("table failed", len(failedTotals)),
		zap.String("log detail", "if exist table failed, please see meta table [wait/full_sync_meta/chunk_error_detail]"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func (r *Migrate) FullPartSyncTable(fullPartTables []string) error {
	taskTime := time.Now()

	g := &errgroup.Group{}
	g.SetLimit(r.Cfg.FullConfig.TableThreads)

	for _, table := range fullPartTables {
		t := table
		g.Go(func() error {
			startTime := time.Now()
			err := meta.NewWaitSyncMetaModel(r.MetaDB).UpdateWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
				TableNameS:  common.StringUPPER(t),
				TaskMode:    r.Cfg.TaskMode,
			}, map[string]interface{}{
				"TaskStatus": common.TaskStatusRunning,
			})
			if err != nil {
				return err
			}

			waitFullMetas, err := meta.NewFullSyncMetaModel(r.MetaDB).DetailFullSyncMeta(r.Ctx, &meta.FullSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
				TableNameS:  common.StringUPPER(t),
				TaskMode:    r.Cfg.TaskMode,
				TaskStatus:  common.TaskStatusWaiting,
			})
			if err != nil {
				return err
			}
			failedFullMetas, err := meta.NewFullSyncMetaModel(r.MetaDB).DetailFullSyncMeta(r.Ctx, &meta.FullSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
				TableNameS:  common.StringUPPER(t),
				TaskMode:    r.Cfg.TaskMode,
				TaskStatus:  common.TaskStatusFailed,
			})
			if err != nil {
				return err
			}

			waitFullMetas = append(waitFullMetas, failedFullMetas...)

			columnNameS, err := r.Oracle.GetOracleTableRowsColumn(
				common.StringsBuilder(`SELECT *`, ` FROM `,
					common.StringUPPER(r.Cfg.OracleConfig.SchemaName), `.`, common.StringUPPER(t), ` WHERE ROWNUM = 1`))
			if err != nil {
				return nil
			}

			g1 := &errgroup.Group{}
			g1.SetLimit(r.Cfg.FullConfig.SQLThreads)
			for _, fullMeta := range waitFullMetas {
				m := fullMeta
				g1.Go(func() error {
					// 数据写入
					err := public.IMigrate(NewRows(r.Ctx, m, r.Oracle, r.Mysql, r.MetaDB, r.Cfg.FullConfig.ApplyThreads, r.Cfg.AppConfig.InsertBatchSize, true, columnNameS))

					if err != nil {
						// record error, skip error
						errf := meta.NewCommonModel(r.MetaDB).UpdateFullSyncMetaChunkAndCreateChunkErrorDetail(r.Ctx, &meta.FullSyncMeta{
							DBTypeS:      m.DBTypeS,
							DBTypeT:      m.DBTypeT,
							SchemaNameS:  m.SchemaNameS,
							TableNameS:   m.TableNameS,
							TaskMode:     m.TaskMode,
							ChunkDetailS: m.ChunkDetailS,
						}, map[string]interface{}{
							"TaskStatus": common.TaskStatusFailed,
						}, &meta.ChunkErrorDetail{
							DBTypeS:      m.DBTypeS,
							DBTypeT:      m.DBTypeT,
							SchemaNameS:  m.SchemaNameS,
							TableNameS:   m.TableNameS,
							SchemaNameT:  m.SchemaNameT,
							TableNameT:   m.TableNameT,
							TaskMode:     m.TaskMode,
							ChunkDetailS: m.ChunkDetailS,
							InfoDetail:   m.String(),
							ErrorDetail:  err.Error(),
						})
						if errf != nil {
							return fmt.Errorf("get oracle schema table [%v] IMigrate failed: %v", m.String(), errf)
						}
						return nil
					}

					if errf := meta.NewFullSyncMetaModel(r.MetaDB).UpdateFullSyncMetaChunk(r.Ctx, &meta.FullSyncMeta{
						DBTypeS:      m.DBTypeS,
						DBTypeT:      m.DBTypeT,
						SchemaNameS:  m.SchemaNameS,
						TableNameS:   m.TableNameS,
						TaskMode:     m.TaskMode,
						ChunkDetailS: m.ChunkDetailS,
					}, map[string]interface{}{
						"TaskStatus": common.TaskStatusSuccess,
					}); errf != nil {
						return fmt.Errorf("get oracle schema table [%v] Success failed: %v", m.String(), errf)
					}
					return nil
				})
			}

			if err = g1.Wait(); err != nil {
				return err
			}

			// 清理元数据记录
			// 更新 wait_sync_meta 记录
			failedChunkTotalErrs, err := meta.NewFullSyncMetaModel(r.MetaDB).CountsErrorFullSyncMeta(r.Ctx, &meta.FullSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
				TableNameS:  common.StringUPPER(t),
				TaskMode:    r.Cfg.TaskMode,
				TaskStatus:  common.TaskStatusFailed,
			})
			if err != nil {
				return fmt.Errorf("get meta table [full_sync_meta] counts failed, error: %v", err)
			}
			successChunkFullMeta, err := meta.NewFullSyncMetaModel(r.MetaDB).DetailFullSyncMeta(r.Ctx, &meta.FullSyncMeta{
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

			// 不存在错误，清理 full_sync_meta 记录, 更新 wait_sync_meta 记录
			if failedChunkTotalErrs == 0 {
				err = meta.NewCommonModel(r.MetaDB).DeleteTableFullSyncMetaAndUpdateWaitSyncMeta(r.Ctx,
					&meta.FullSyncMeta{
						DBTypeS:     r.Cfg.DBTypeS,
						DBTypeT:     r.Cfg.DBTypeT,
						SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
						TableNameS:  common.StringUPPER(t),
						TaskMode:    r.Cfg.TaskMode,
					}, &meta.WaitSyncMeta{
						DBTypeS:          r.Cfg.DBTypeS,
						DBTypeT:          r.Cfg.DBTypeT,
						SchemaNameS:      common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
						TableNameS:       common.StringUPPER(t),
						TaskMode:         r.Cfg.TaskMode,
						TaskStatus:       common.TaskStatusSuccess,
						ChunkSuccessNums: int64(len(successChunkFullMeta)),
						ChunkFailedNums:  0,
					})
				if err != nil {
					return err
				}
				zap.L().Info("full single table oracle to mysql finished",
					zap.String("schema", r.Cfg.OracleConfig.SchemaName),
					zap.String("table", common.StringUPPER(t)),
					zap.String("cost", time.Now().Sub(startTime).String()))
			} else {
				// 若存在错误，修改表状态，skip 清理，统一忽略，最后显示
				err = meta.NewWaitSyncMetaModel(r.MetaDB).UpdateWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
					DBTypeS:     r.Cfg.DBTypeS,
					DBTypeT:     r.Cfg.DBTypeT,
					SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:  common.StringUPPER(t),
					TaskMode:    r.Cfg.TaskMode,
				}, map[string]interface{}{
					"TaskStatus":       common.TaskStatusFailed,
					"ChunkSuccessNums": int64(len(successChunkFullMeta)),
					"ChunkFailedNums":  failedChunkTotalErrs,
				})
				if err != nil {
					return err
				}
				zap.L().Warn("update mysql [wait_sync_meta] meta",
					zap.String("schema", r.Cfg.OracleConfig.SchemaName),
					zap.String("table", common.StringUPPER(t)),
					zap.String("mode", r.Cfg.TaskMode),
					zap.String("updated", "table exist error, skip"),
					zap.String("cost", time.Now().Sub(startTime).String()))
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	zap.L().Info("source schema all table data loader finished",
		zap.String("schema", r.Cfg.OracleConfig.SchemaName),
		zap.Int("table totals", len(fullPartTables)),
		zap.String("cost", time.Now().Sub(taskTime).String()))
	return nil
}

func (r *Migrate) FullWaitSyncTable(fullWaitTables []string, oracleCollation bool) error {
	err := r.InitWaitSyncTableChunk(fullWaitTables, oracleCollation)
	if err != nil {
		return err
	}
	err = r.FullPartSyncTable(fullWaitTables)
	if err != nil {
		return err
	}

	return nil
}

func (r *Migrate) InitWaitSyncTableChunk(csvWaitTables []string, oracleCollation bool) error {
	startTask := time.Now()
	// 获取自定义库表名规则
	tableNameRule, err := r.GetTableNameRule()
	if err != nil {
		return err
	}

	// 全量同步前，获取 SCN 以及初始化元数据表
	globalSCN, err := r.Oracle.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}
	partitionTables, err := r.Oracle.GetOracleSchemaPartitionTable(r.Cfg.OracleConfig.SchemaName)
	if err != nil {
		return err
	}

	g := &errgroup.Group{}
	g.SetLimit(r.Cfg.FullConfig.TaskThreads)

	for idx, table := range csvWaitTables {
		t := table
		workerID := idx
		g.Go(func() error {
			startTime := time.Now()
			// 库名、表名规则
			var targetTableName string
			if val, ok := tableNameRule[common.StringUPPER(t)]; ok {
				targetTableName = val
			} else {
				targetTableName = common.StringUPPER(t)
			}

			sourceColumnInfo, err := r.AdjustTableSelectColumn(t, oracleCollation)
			if err != nil {
				return err
			}

			var (
				isPartition string
			)
			if common.IsContainString(partitionTables, common.StringUPPER(t)) {
				isPartition = "YES"
			} else {
				isPartition = "NO"
			}

			tableRowsByStatistics, err := r.Oracle.GetOracleTableRowsByStatistics(r.Cfg.OracleConfig.SchemaName, t)
			if err != nil {
				return err
			}
			// 统计信息数据行数 0，直接全表扫
			if tableRowsByStatistics == 0 {
				err = meta.NewCommonModel(r.MetaDB).CreateFullSyncMetaAndUpdateWaitSyncMeta(r.Ctx, &meta.FullSyncMeta{
					DBTypeS:       r.Cfg.DBTypeS,
					DBTypeT:       r.Cfg.DBTypeT,
					SchemaNameS:   common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:    common.StringUPPER(t),
					SchemaNameT:   common.StringUPPER(r.Cfg.MySQLConfig.SchemaName),
					TableNameT:    common.StringUPPER(targetTableName),
					GlobalScnS:    globalSCN,
					ColumnDetailS: sourceColumnInfo,
					ChunkDetailS:  "1 = 1",
					TaskMode:      r.Cfg.TaskMode,
					TaskStatus:    common.TaskStatusWaiting,
				}, &meta.WaitSyncMeta{
					DBTypeS:          r.Cfg.DBTypeS,
					DBTypeT:          r.Cfg.DBTypeT,
					SchemaNameS:      common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:       common.StringUPPER(t),
					TaskMode:         r.Cfg.TaskMode,
					GlobalScnS:       globalSCN,
					TableNumRows:     uint64(tableRowsByStatistics),
					ChunkTotalNums:   1,
					ChunkSuccessNums: 0,
					ChunkFailedNums:  0,
					IsPartition:      isPartition,
				})
				if err != nil {
					return err
				}
				return nil
			}

			taskName := common.StringsBuilder(common.StringUPPER(r.Cfg.OracleConfig.SchemaName), `_`, common.StringUPPER(t), `_`, `TASK`, strconv.Itoa(workerID))

			if err = r.Oracle.StartOracleChunkCreateTask(taskName); err != nil {
				return err
			}

			if err = r.Oracle.StartOracleCreateChunkByRowID(taskName, common.StringUPPER(r.Cfg.OracleConfig.SchemaName), common.StringUPPER(t), strconv.Itoa(r.Cfg.CSVConfig.Rows)); err != nil {
				return err
			}

			chunkRes, err := r.Oracle.GetOracleTableChunksByRowID(taskName)
			if err != nil {
				return err
			}

			// 判断数据是否存在
			if len(chunkRes) == 0 {
				err = meta.NewCommonModel(r.MetaDB).CreateFullSyncMetaAndUpdateWaitSyncMeta(r.Ctx, &meta.FullSyncMeta{
					DBTypeS:       r.Cfg.DBTypeS,
					DBTypeT:       r.Cfg.DBTypeT,
					SchemaNameS:   common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:    common.StringUPPER(t),
					SchemaNameT:   common.StringUPPER(r.Cfg.MySQLConfig.SchemaName),
					TableNameT:    common.StringUPPER(targetTableName),
					GlobalScnS:    globalSCN,
					ColumnDetailS: sourceColumnInfo,
					ChunkDetailS:  "1 = 1",
					TaskMode:      r.Cfg.TaskMode,
					TaskStatus:    common.TaskStatusWaiting,
				}, &meta.WaitSyncMeta{
					DBTypeS:          r.Cfg.DBTypeS,
					DBTypeT:          r.Cfg.DBTypeT,
					SchemaNameS:      common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:       common.StringUPPER(t),
					TaskMode:         r.Cfg.TaskMode,
					TableNumRows:     uint64(tableRowsByStatistics),
					GlobalScnS:       globalSCN,
					ChunkTotalNums:   1,
					ChunkSuccessNums: 0,
					ChunkFailedNums:  0,
					IsPartition:      isPartition,
				})
				if err != nil {
					return err
				}

				return nil
			}

			var fullMetas []meta.FullSyncMeta
			for _, res := range chunkRes {
				fullMetas = append(fullMetas, meta.FullSyncMeta{
					DBTypeS:       r.Cfg.DBTypeS,
					DBTypeT:       r.Cfg.DBTypeT,
					SchemaNameS:   common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
					TableNameS:    common.StringUPPER(t),
					SchemaNameT:   common.StringUPPER(r.Cfg.MySQLConfig.SchemaName),
					TableNameT:    common.StringUPPER(targetTableName),
					GlobalScnS:    globalSCN,
					ColumnDetailS: sourceColumnInfo,
					ChunkDetailS:  res["CMD"],
					TaskMode:      r.Cfg.TaskMode,
					TaskStatus:    common.TaskStatusWaiting,
				})
			}

			// 元数据库信息 batch 写入
			err = meta.NewFullSyncMetaModel(r.MetaDB).BatchCreateFullSyncMeta(r.Ctx, fullMetas, r.Cfg.AppConfig.InsertBatchSize)
			if err != nil {
				return err
			}

			// 更新 wait_sync_meta
			err = meta.NewWaitSyncMetaModel(r.MetaDB).UpdateWaitSyncMeta(r.Ctx, &meta.WaitSyncMeta{
				DBTypeS:     r.Cfg.DBTypeS,
				DBTypeT:     r.Cfg.DBTypeT,
				SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
				TableNameS:  common.StringUPPER(t),
				TaskMode:    r.Cfg.TaskMode,
			}, map[string]interface{}{
				"TableNumRows":     uint64(tableRowsByStatistics),
				"GlobalScnS":       globalSCN,
				"ChunkTotalNums":   len(chunkRes),
				"ChunkSuccessNums": 0,
				"ChunkFailedNums":  0,
				"IsPartition":      isPartition,
			})
			if err != nil {
				return err
			}

			if err = r.Oracle.CloseOracleChunkTask(taskName); err != nil {
				return err
			}

			endTime := time.Now()
			zap.L().Info("init source single table wait_sync_meta and full_sync_meta finished",
				zap.String("schema", r.Cfg.OracleConfig.SchemaName),
				zap.String("table", t),
				zap.String("cost", endTime.Sub(startTime).String()))
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	zap.L().Info("init source schema table wait_sync_meta and full_sync_meta finished",
		zap.String("schema", r.Cfg.OracleConfig.SchemaName),
		zap.String("cost", time.Now().Sub(startTask).String()))
	return nil
}

func (r *Migrate) GetTableNameRule() (map[string]string, error) {
	// 获取表名自定义规则
	tableNameRules, err := meta.NewTableNameRuleModel(r.MetaDB).DetailTableNameRule(r.Ctx, &meta.TableNameRule{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: r.Cfg.OracleConfig.SchemaName,
		SchemaNameT: r.Cfg.MySQLConfig.SchemaName,
	})
	if err != nil {
		return nil, err
	}
	tableNameRuleMap := make(map[string]string)

	if len(tableNameRules) > 0 {
		for _, tr := range tableNameRules {
			tableNameRuleMap[common.StringUPPER(tr.TableNameS)] = common.StringUPPER(tr.TableNameT)
		}
	}
	return tableNameRuleMap, nil
}

func (r *Migrate) AdjustTableSelectColumn(sourceTable string, oracleCollation bool) (string, error) {
	// Date/Timestamp 字段类型格式化
	// Interval Year/Day 数据字符 TO_CHAR 格式化
	columnsINFO, err := r.Oracle.GetOracleSchemaTableColumn(r.Cfg.OracleConfig.SchemaName, sourceTable, oracleCollation)
	if err != nil {
		return "", err
	}

	var columnNames []string

	for _, rowCol := range columnsINFO {
		switch strings.ToUpper(rowCol["DATA_TYPE"]) {
		// 数字
		case "NUMBER":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		case "DECIMAL", "DEC", "DOUBLE PRECISION", "FLOAT", "INTEGER", "INT", "REAL", "NUMERIC", "BINARY_FLOAT", "BINARY_DOUBLE", "SMALLINT":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		// 字符
		case "BFILE", "CHARACTER", "LONG", "NCHAR VARYING", "ROWID", "UROWID", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR2", "NCLOB", "CLOB":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		// XMLTYPE
		case "XMLTYPE":
			columnNames = append(columnNames, fmt.Sprintf(" XMLSERIALIZE(CONTENT %s AS CLOB) AS %s", rowCol["COLUMN_NAME"], rowCol["COLUMN_NAME"]))
		// 二进制
		case "BLOB", "LONG RAW", "RAW":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		// 时间
		case "DATE":
			columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-MM-dd HH24:mi:ss') AS ", rowCol["COLUMN_NAME"]))
		// 默认其他类型
		default:
			if strings.Contains(rowCol["DATA_TYPE"], "INTERVAL") {
				columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ") AS ", rowCol["COLUMN_NAME"]))
			} else if strings.Contains(rowCol["DATA_TYPE"], "TIMESTAMP") {
				dataScale, err := strconv.Atoi(rowCol["DATA_SCALE"])
				if err != nil {
					return "", fmt.Errorf("aujust oracle timestamp datatype scale [%s] strconv.Atoi failed: %v", rowCol["DATA_SCALE"], err)
				}
				if dataScale == 0 {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-mm-dd hh24:mi:ss') AS ", rowCol["COLUMN_NAME"]))
				} else if dataScale < 0 && dataScale <= 6 {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"],
						",'yyyy-mm-dd hh24:mi:ss.ff", rowCol["DATA_SCALE"], "') AS ", rowCol["COLUMN_NAME"]))
				} else {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-mm-dd hh24:mi:ss.ff6') AS ", rowCol["COLUMN_NAME"]))
				}

			} else {
				columnNames = append(columnNames, rowCol["COLUMN_NAME"])
			}
		}

	}

	return strings.Join(columnNames, ","), nil
}
