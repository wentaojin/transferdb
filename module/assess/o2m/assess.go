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
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/module/query/oracle"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Report struct {
	ctx    context.Context
	cfg    *config.Config
	oracle *oracle.Oracle
}

func NewReport(ctx context.Context, cfg *config.Config, oracle *oracle.Oracle) *Report {
	return &Report{
		ctx:    ctx,
		cfg:    cfg,
		oracle: oracle,
	}
}

func (r *Report) Assess() error {
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

	beginTime := time.Now()
	reportOverview, reportSchema, err := GatherOracleOverview(usernameArray, r.oracle, r.cfg.OracleConfig.Username, fileName)
	if err != nil {
		return err
	}
	finishedTime := time.Now()
	zap.L().Info("gather database overview",
		zap.Strings("schema", usernameArray),
		zap.String("cost", finishedTime.Sub(beginTime).String()))

	beginTime = time.Now()
	reportType, err := GatherOracleType(usernameArray, r.oracle)
	if err != nil {
		return err
	}
	finishedTime = time.Now()
	zap.L().Info("gather database type",
		zap.Strings("schema", usernameArray),
		zap.String("cost", finishedTime.Sub(beginTime).String()))

	beginTime = time.Now()
	reportCheck, err := GatherOracleCheck(usernameArray, r.oracle)
	if err != nil {
		return err
	}
	finishedTime = time.Now()
	zap.L().Info("gather database check",
		zap.Strings("schema", usernameArray),
		zap.String("cost", finishedTime.Sub(beginTime).String()))

	if err = GenNewHTMLReport(reportOverview, reportSchema, reportType, reportCheck, file); err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("assess oracle migrate myoracle cost finished",
		zap.String("cost", endTime.Sub(startTime).String()),
		zap.String("output", filepath.Join(pwdDir, fileName)))
	return nil
}

func GatherOracleOverview(schemaName []string, oracle *oracle.Oracle, reportUser, reportName string) (*ReportOverview, *ReportSchema, error) {

	reportOverview, err := GatherOracleDBOverview(oracle, reportName, reportUser)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listActiveSessionCount, err := GatherOracleMaxActiveSessionCount(oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listSchemaTableSizeData, err := GatherOracleSchemaOverview(schemaName, oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listSchemaTableRowsTOP, err := GatherOracleSchemaTableRowsTOP(schemaName, oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listSchemaObject, err := GatherOracleSchemaObjectOverview(schemaName, oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listPartitionType, err := GatherOracleSchemaPartitionType(schemaName, oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listColumnType, err := GatherOracleSchemaColumnTypeAndMaxLength(schemaName, oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listTableRow, err := GatherOracleSchemaTableAvgRowLength(schemaName, oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listTemporary, err := GatherOracleSchemaTemporaryTable(schemaName, oracle)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	return reportOverview, &ReportSchema{
		ListSchemaActiveSession:               listActiveSessionCount,
		ListSchemaTableSizeData:               listSchemaTableSizeData,
		ListSchemaTableRowsTOP:                listSchemaTableRowsTOP,
		ListSchemaTableObjectCounts:           listSchemaObject,
		ListSchemaTablePartitionType:          listPartitionType,
		ListSchemaTableColumnTypeAndMaxLength: listColumnType,
		ListSchemaTableAvgRowLength:           listTableRow,
		ListSchemaTemporaryTableCounts:        listTemporary,
	}, nil
}

func GatherOracleType(schemaName []string, oracle *oracle.Oracle) (*ReportType, error) {

	listIndexType, err := GatherOracleSchemaIndexType(schemaName, oracle)
	if err != nil {
		return &ReportType{}, err
	}

	listConstraintType, err := GatherOracleConstraintType(schemaName, oracle)
	if err != nil {
		return &ReportType{}, err
	}

	listCodeType, err := GatherOracleSchemeCodeType(schemaName, oracle)
	if err != nil {
		return &ReportType{}, err
	}

	listSynonymType, err := GatherOracleSchemaSynonymType(schemaName, oracle)
	if err != nil {
		return &ReportType{}, err
	}

	return &ReportType{
		ListSchemaIndexType:      listIndexType,
		ListSchemaConstraintType: listConstraintType,
		ListSchemaCodeType:       listCodeType,
		ListSchemaSynonymType:    listSynonymType,
	}, nil
}

func GatherOracleCheck(schemaName []string, oracle *oracle.Oracle) (*ReportCheck, error) {

	listPartitionTableCountsCK, err := GatherOraclePartitionTableCountsCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableRowLengthCK, err := GatherOracleTableRowLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	ListIndexRowLengthCK, err := GatherOracleTableIndexRowLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableColumnCountsCK, err := GatherOracleTableColumnCountsCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableIndexCountsCK, err := GatherOracleTableIndexCountsCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableNumberCK, err := GatherOracleTableNumberTypeCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listUsernameCK, err := GatherOracleUsernameLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableNameLengthCK, err := GatherOracleTableNameLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listColumnLengthCK, err := GatherOracleColumnNameLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listIndexLengthCK, err := GatherOracleIndexNameLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listViewLengthCK, err := GatherOracleViewNameLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	listSeqLength, err := GatherOracleSequenceNameLengthCheck(schemaName, oracle)
	if err != nil {
		return &ReportCheck{}, err
	}

	return &ReportCheck{
		ListSchemaPartitionTableCountsCheck:  listPartitionTableCountsCK,
		ListSchemaTableRowLengthCheck:        listTableRowLengthCK,
		ListSchemaTableIndexRowLengthCheck:   ListIndexRowLengthCK,
		ListSchemaTableColumnCountsCheck:     listTableColumnCountsCK,
		ListSchemaIndexCountsCheck:           listTableIndexCountsCK,
		ListSchemaTableNumberTypeCheck:       listTableNumberCK,
		ListUsernameLengthCheck:              listUsernameCK,
		ListSchemaTableNameLengthCheck:       listTableNameLengthCK,
		ListSchemaTableColumnNameLengthCheck: listColumnLengthCK,
		ListSchemaTableIndexNameLengthCheck:  listIndexLengthCK,
		ListSchemaViewNameLengthCheck:        listViewLengthCK,
		ListSchemaSequenceNameLengthCheck:    listSeqLength,
	}, err
}
