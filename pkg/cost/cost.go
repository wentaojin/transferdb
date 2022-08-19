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
package cost

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wentaojin/transferdb/service"
	"go.uber.org/zap"
)

func OracleMigrateMySQLCostEvaluate(engine *service.Engine, cfg *service.CfgFile) error {
	startTime := time.Now()
	zap.L().Info("evaluate oracle migrate mysql cost start",
		zap.String("oracleSchema", cfg.SourceConfig.SchemaName),
		zap.String("mysqlSchema", cfg.TargetConfig.SchemaName))

	var (
		usernameSQL   string
		fileName      string
		usernameArray []string
	)
	if cfg.SourceConfig.SchemaName == "" {
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
		usernameSQL = fmt.Sprintf(`select username from dba_users where username = '%s'`, strings.ToUpper(cfg.SourceConfig.SchemaName))
		fileName = fmt.Sprintf("report_%s.html", cfg.SourceConfig.SchemaName)
	}
	_, usernameMapArray, err := service.Query(engine.OracleDB, usernameSQL)
	if err != nil {
		return err
	}

	if len(usernameMapArray) == 0 {
		return fmt.Errorf("oracle schema [%v] not exist", strings.ToUpper(cfg.SourceConfig.SchemaName))
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
	reportOverview, reportSchema, err := GatherOracleOverview(usernameArray, engine, cfg.SourceConfig.Username, fileName)
	if err != nil {
		return err
	}
	finishedTime := time.Now()
	zap.L().Info("gather database overview",
		zap.Strings("schema", usernameArray),
		zap.String("cost", finishedTime.Sub(beginTime).String()))

	beginTime = time.Now()
	reportType, err := GatherOracleType(usernameArray, engine)
	if err != nil {
		return err
	}
	finishedTime = time.Now()
	zap.L().Info("gather database type",
		zap.Strings("schema", usernameArray),
		zap.String("cost", finishedTime.Sub(beginTime).String()))

	beginTime = time.Now()
	reportCheck, err := GatherOracleCheck(usernameArray, engine)
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
	zap.L().Info("evaluate oracle migrate mysql cost finished",
		zap.String("cost", endTime.Sub(startTime).String()),
		zap.String("output", filepath.Join(pwdDir, fileName)))
	return nil
}

func GatherOracleOverview(schemaName []string, engine *service.Engine, reportUser, reportName string) (*ReportOverview, *ReportSchema, error) {

	reportOverview, err := GatherOracleDBOverview(engine, reportName, reportUser)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listActiveSessionCount, err := GatherOracleMaxActiveSessionCount(engine)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listSchemaTableSizeData, err := GatherOracleSchemaOverview(schemaName, engine)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listSchemaTableRowsTOP, err := GatherOracleSchemaTableRowsTOP(schemaName, engine)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listSchemaObject, err := GatherOracleSchemaObjectOverview(schemaName, engine)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listPartitionType, err := GatherOracleSchemaPartitionType(schemaName, engine)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listColumnType, err := GatherOracleSchemaColumnTypeAndMaxLength(schemaName, engine)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listTableRow, err := GatherOracleSchemaTableAvgRowLength(schemaName, engine)
	if err != nil {
		return &ReportOverview{}, &ReportSchema{}, err
	}

	listTemporary, err := GatherOracleSchemaTemporaryTable(schemaName, engine)
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

func GatherOracleType(schemaName []string, engine *service.Engine) (*ReportType, error) {

	listIndexType, err := GatherOracleSchemaIndexType(schemaName, engine)
	if err != nil {
		return &ReportType{}, err
	}

	listConstraintType, err := GatherOracleConstraintType(schemaName, engine)
	if err != nil {
		return &ReportType{}, err
	}

	listCodeType, err := GatherOracleSchemeCodeType(schemaName, engine)
	if err != nil {
		return &ReportType{}, err
	}

	listSynonymType, err := GatherOracleSchemaSynonymType(schemaName, engine)
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

func GatherOracleCheck(schemaName []string, engine *service.Engine) (*ReportCheck, error) {

	listPartitionTableCountsCK, err := GatherOraclePartitionTableCountsCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableRowLengthCK, err := GatherOracleTableRowLengthCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	ListIndexRowLengthCK, err := GatherOracleTableIndexRowLengthCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableColumnCountsCK, err := GatherOracleTableColumnCountsCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableIndexCountsCK, err := GatherOracleTableIndexCountsCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableNumberCK, err := GatherOracleTableNumberTypeCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listUsernameCK, err := GatherOracleUsernameLengthCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listTableNameLengthCK, err := GatherOracleTableNameLengthCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listColumnLengthCK, err := GatherOracleColumnNameLengthCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listIndexLengthCK, err := GatherOracleIndexNameLengthCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listViewLengthCK, err := GatherOracleViewNameLengthCheck(schemaName, engine)
	if err != nil {
		return &ReportCheck{}, err
	}

	listSeqLength, err := GatherOracleSequenceNameLengthCheck(schemaName, engine)
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
