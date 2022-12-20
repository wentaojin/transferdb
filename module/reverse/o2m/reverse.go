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
	"github.com/wentaojin/transferdb/module/reverse"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type O2M struct {
	ctx    context.Context
	cfg    *config.Config
	mysql  *mysql.MySQL
	oracle *oracle.Oracle
	metaDB *meta.Meta
}

func NewO2MReverse(ctx context.Context, cfg *config.Config, mysql *mysql.MySQL, oracle *oracle.Oracle, metaDB *meta.Meta) *O2M {
	return &O2M{
		ctx:    ctx,
		cfg:    cfg,
		mysql:  mysql,
		oracle: oracle,
		metaDB: metaDB,
	}
}

func (o2m *O2M) NewReverse() error {
	startTime := time.Now()
	zap.L().Info("reverse table o2m.oracle to mysql start",
		zap.String("schema", o2m.cfg.OracleConfig.SchemaName))

	// 获取配置文件待同步表列表
	exporters, err := filterCFGTable(o2m.cfg, o2m.oracle)
	if err != nil {
		return err
	}

	if len(exporters) == 0 {
		zap.L().Warn("there are no table objects in the o2m.oracle schema",
			zap.String("schema", o2m.cfg.OracleConfig.SchemaName))
		return nil
	}

	// 判断 table_error_detail 是否存在错误记录，是否可进行 reverse
	errTotals, err := meta.NewTableErrorDetailModel(o2m.metaDB).CountsBySchema(o2m.ctx, &meta.TableErrorDetail{
		SourceSchemaName: common.StringUPPER(o2m.cfg.OracleConfig.SchemaName),
		RunMode:          common.ReverseO2MMode,
	})
	if errTotals > 0 || err != nil {
		return fmt.Errorf("reverse schema [%s] table mode [%s] task failed: %v, table [table_error_detail] exist failed error, please clear and rerunning", o2m.cfg.OracleConfig.SchemaName, common.ReverseO2MMode, err)
	}

	// 获取 o2m.oracle 数据库字符排序规则
	nlsComp, err := o2m.oracle.GetOracleDBCharacterNLSCompCollation()
	if err != nil {
		return err
	}
	nlsSort, err := o2m.oracle.GetOracleDBCharacterNLSSortCollation()
	if err != nil {
		return err
	}
	if _, ok := common.OracleCollationMap[common.StringUPPER(nlsComp)]; !ok {
		return fmt.Errorf("o2m.oracle db nls comp [%s] , mysql db isn't support", nlsComp)
	}
	if _, ok := common.OracleCollationMap[common.StringUPPER(nlsSort)]; !ok {
		return fmt.Errorf("o2m.oracle db nls sort [%s] , mysql db isn't support", nlsSort)
	}

	if !strings.EqualFold(nlsSort, nlsComp) {
		return fmt.Errorf("o2m.oracle db nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// 筛选过滤可能不支持的表类型
	partitionTables, err := filterOraclePartitionTable(o2m.cfg, o2m.oracle, exporters)
	if err != nil {
		return fmt.Errorf("error on filter o2m.oracle partition table: %v", err)
	}
	temporaryTables, err := filterOracleTemporaryTable(o2m.cfg, o2m.oracle, exporters)
	if err != nil {
		return fmt.Errorf("error on filter o2m.oracle temporary table: %v", err)

	}
	clusteredTables, err := filterOracleClusteredTable(o2m.cfg, o2m.oracle, exporters)
	if err != nil {
		return fmt.Errorf("error on filter o2m.oracle clustered table: %v", err)

	}
	materializedView, err := filterOracleMaterializedView(o2m.cfg, o2m.oracle, exporters)
	if err != nil {
		return fmt.Errorf("error on filter o2m.oracle materialized view: %v", err)

	}

	if len(partitionTables) != 0 {
		zap.L().Warn("partition tables",
			zap.String("schema", o2m.cfg.OracleConfig.SchemaName),
			zap.String("partition table list", fmt.Sprintf("%v", partitionTables)),
			zap.String("suggest", "if necessary, please manually convert and process the tables in the above list"))
	}
	if len(temporaryTables) != 0 {
		zap.L().Warn("temporary tables",
			zap.String("schema", o2m.cfg.OracleConfig.SchemaName),
			zap.String("temporary table list", fmt.Sprintf("%v", temporaryTables)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}
	if len(clusteredTables) != 0 {
		zap.L().Warn("clustered tables",
			zap.String("schema", o2m.cfg.OracleConfig.SchemaName),
			zap.String("clustered table list", fmt.Sprintf("%v", clusteredTables)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}

	var exporterTables []string
	if len(materializedView) != 0 {
		zap.L().Warn("materialized views",
			zap.String("schema", o2m.cfg.OracleConfig.SchemaName),
			zap.String("materialized view list", fmt.Sprintf("%v", materializedView)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))

		// 排除物化视图
		exporterTables = common.FilterDifferenceStringItems(exporters, materializedView)
	} else {
		exporterTables = exporters
	}

	// 获取 reverse 表任务列表
	tables, err := GenReverseTableTask(o2m.ctx, o2m.cfg, o2m.mysql, o2m.oracle, exporterTables, nlsSort, nlsComp)
	if err != nil {
		return err
	}

	pwdDir, err := os.Getwd()
	if err != nil {
		return err
	}

	reverseFile := filepath.Join(pwdDir, fmt.Sprintf("reverse_%s.sql", o2m.cfg.OracleConfig.SchemaName))
	compFile := filepath.Join(pwdDir, fmt.Sprintf("compatibility_%s.sql", o2m.cfg.OracleConfig.SchemaName))

	// file writer
	f, err := reverse.NewWriter(reverseFile, compFile, o2m.mysql, o2m.oracle)
	if err != nil {
		return err
	}

	// schema create
	err = GenCreateSchema(f,
		common.StringUPPER(o2m.cfg.OracleConfig.SchemaName), common.StringUPPER(o2m.cfg.MySQLConfig.SchemaName), nlsComp)
	if err != nil {
		return err
	}

	// 表类型不兼容项输出
	err = GenCompatibilityTable(f, common.StringUPPER(o2m.cfg.OracleConfig.SchemaName), partitionTables, temporaryTables, clusteredTables, materializedView)
	if err != nil {
		return err
	}

	// 表转换
	g := &errgroup.Group{}
	g.SetLimit(o2m.cfg.AppConfig.Threads)

	for _, table := range tables {
		t := table
		g.Go(func() error {
			rule, err := IReader(o2m.ctx, o2m.metaDB, t, t)
			if err != nil {
				if err = meta.NewTableErrorDetailModel(o2m.metaDB).Create(o2m.ctx, &meta.TableErrorDetail{
					SourceSchemaName: t.SourceSchemaName,
					SourceTableName:  t.SourceTableName,
					RunMode:          common.ReverseO2MMode,
					InfoSources:      common.ReverseO2MMode,
					RunStatus:        "Failed",
					InfoDetail:       t.String(),
					ErrorDetail:      err.Error(),
				}); err != nil {
					zap.L().Error("reverse table o2m.oracle to mysql failed",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("reader table task failed, detail see [table_error_detail], please rerunning")))

					return fmt.Errorf("reader table task failed, detail see [table_error_detail], please rerunning, error: %v", err)
				}
				return nil
			}
			ddl, err := IReverse(t, rule)
			if err != nil {
				if err = meta.NewTableErrorDetailModel(o2m.metaDB).Create(o2m.ctx, &meta.TableErrorDetail{
					SourceSchemaName: t.SourceSchemaName,
					SourceTableName:  t.SourceTableName,
					RunMode:          common.ReverseO2MMode,
					InfoSources:      common.ReverseO2MMode,
					RunStatus:        "Failed",
					InfoDetail:       t.String(),
					ErrorDetail:      err.Error(),
				}); err != nil {
					zap.L().Error("reverse table o2m.oracle to mysql failed",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("reverse table task failed, detail see [table_error_detail], please rerunning")))

					return fmt.Errorf("reverse table task failed, detail see [table_error_detail], please rerunning, error: %v", err)
				}
				return nil
			}

			err = IWriter(f, ddl)
			if err != nil {
				if err = meta.NewTableErrorDetailModel(o2m.metaDB).Create(o2m.ctx, &meta.TableErrorDetail{
					SourceSchemaName: t.SourceSchemaName,
					SourceTableName:  t.SourceTableName,
					RunMode:          common.ReverseO2MMode,
					InfoSources:      common.ReverseO2MMode,
					RunStatus:        "Failed",
					InfoDetail:       t.String(),
					ErrorDetail:      err.Error(),
				}); err != nil {
					zap.L().Error("reverse table o2m.oracle to mysql failed",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("writer table task failed, detail see [table_error_detail], please rerunning")))

					return fmt.Errorf("writer table task failed, detail see [table_error_detail], please rerunning, error: %v", err)
				}
				return nil
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	errTotals, err = meta.NewTableErrorDetailModel(o2m.metaDB).CountsBySchema(o2m.ctx, &meta.TableErrorDetail{
		SourceSchemaName: common.StringUPPER(o2m.cfg.OracleConfig.SchemaName),
		RunMode:          common.ReverseO2MMode,
	})
	if err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("reverse", zap.String("create table and index output", filepath.Join(pwdDir,
		fmt.Sprintf("reverse_%s.sql", o2m.cfg.OracleConfig.SchemaName))))
	zap.L().Info("compatibility", zap.String("maybe exist compatibility output", filepath.Join(pwdDir,
		fmt.Sprintf("compatibility_%s.sql", o2m.cfg.OracleConfig.SchemaName))))
	if errTotals == 0 {
		zap.L().Info("reverse table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("reverse totals", len(tables)),
			zap.Int("reverse success", len(tables)),
			zap.Int64("reverse failed", errTotals),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		zap.L().Warn("reverse table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("reverse totals", len(tables)),
			zap.Int("reverse success", len(tables)-int(errTotals)),
			zap.Int64("reverse failed", errTotals),
			zap.String("failed tips", "failed detail, please see table [table_error_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil
}
