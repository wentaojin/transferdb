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
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Assess struct {
	ctx    context.Context
	cfg    *config.Config
	metaDB *meta.Meta
	oracle *oracle.Oracle
}

func NewAssess(ctx context.Context, cfg *config.Config) (*Assess, error) {
	oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}
	metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return nil, err
	}
	return &Assess{
		ctx:    ctx,
		cfg:    cfg,
		metaDB: metaDB,
		oracle: oracleDB,
	}, nil
}

func (r *Assess) Assess() error {
	startTime := time.Now()
	zap.L().Info("assess oracle migrate myoracle cost start",
		zap.String("oracleSchema", r.cfg.OracleConfig.SchemaName),
		zap.String("myoracleSchema", r.cfg.MySQLConfig.SchemaName))

	var (
		usernameSQL   string
		fileName      string
		usernameArray []string
	)
	if r.cfg.OracleConfig.SchemaName == "" {
		usernameSQL = `select username from dba_users where username NOT IN (
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

		fileName = "report_all.html"
	} else {
		usernameSQL = fmt.Sprintf(`select username from dba_users where username = '%s'`, strings.ToUpper(r.cfg.OracleConfig.SchemaName))
		fileName = fmt.Sprintf("report_%s.html", r.cfg.OracleConfig.ServiceName)
	}
	_, usernameMapArray, err := oracle.Query(r.ctx, r.oracle.OracleDB, usernameSQL)
	if err != nil {
		return err
	}

	if len(usernameMapArray) == 0 {
		return fmt.Errorf("oracle schema [%v] not exist", strings.ToUpper(r.cfg.OracleConfig.SchemaName))
	}

	for _, usernameMap := range usernameMapArray {
		usernameArray = append(usernameArray, fmt.Sprintf("'%s'", usernameMap["USERNAME"]))
	}

	zap.L().Info("gather database schema array", zap.Strings("schema", usernameArray))

	pwdDir, err := os.Getwd()
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filepath.Join(pwdDir, fileName), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	// 评估
	beginTime := time.Now()
	report, err := GetAssessDatabaseReport(r.ctx, r.metaDB, r.oracle, usernameArray, fileName, common.StringUPPER(r.cfg.OracleConfig.Username), r.cfg.DBTypeS, r.cfg.DBTypeT)
	if err != nil {
		return err
	}
	finishedTime := time.Now()
	zap.L().Info("assess database result finish",
		zap.Strings("schema", usernameArray),
		zap.String("cost", finishedTime.Sub(beginTime).String()))

	startHTMLTime := time.Now()
	if err = GenNewHTMLReport(report, file); err != nil {
		return err
	}
	finishHTMLTime := time.Now()
	zap.L().Info("get database result from db finish",
		zap.Strings("schema", usernameArray),
		zap.String("cost", finishHTMLTime.Sub(startHTMLTime).String()))

	endTime := time.Now()
	zap.L().Info("assess oracle migrate myoracle cost finished",
		zap.String("cost", endTime.Sub(startTime).String()),
		zap.String("output", filepath.Join(pwdDir, fileName)))
	return nil
}
