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

	"golang.org/x/sync/errgroup"

	"github.com/wentaojin/transferdb/service"
	"go.uber.org/zap"
)

func OracleMigrateMySQLCostEvaluate(engine *service.Engine, cfg *service.CfgFile) error {
	startTime := time.Now()
	service.Logger.Info("evaluate oracle migrate mysql cost start",
		zap.String("oracleSchema", cfg.SourceConfig.SchemaName),
		zap.String("mysqlSchema", cfg.TargetConfig.SchemaName))

	var (
		usernameSQL   string
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
	} else {
		usernameSQL = fmt.Sprintf(`select username from dba_users where username = '%s'`, strings.ToUpper(cfg.SourceConfig.SchemaName))
	}
	_, usernameMapArray, err := service.Query(engine.OracleDB, usernameSQL)
	if err != nil {
		return err
	}
	for _, usernameMap := range usernameMapArray {
		usernameArray = append(usernameArray, fmt.Sprintf("'%s'", usernameMap["USERNAME"]))
	}

	var eg errgroup.Group

	overviewChan := make(chan string)
	typeChan := make(chan string)
	checkChan := make(chan string)

	eg.Go(func() error {
		overviewOracle, err := GatherOracleOverview(usernameArray, engine)
		if err != nil {
			return err
		}
		overviewChan <- overviewOracle
		return nil
	})

	eg.Go(func() error {
		typeOracle, err := GatherOracleType(usernameArray, engine)
		if err != nil {
			return err
		}
		typeChan <- typeOracle
		return nil
	})

	eg.Go(func() error {
		checkOracle, err := GatherOracleCheck(usernameArray, engine)
		if err != nil {
			return err
		}
		checkChan <- checkOracle
		return nil
	})

	var builder strings.Builder
	for i := 0; i < 3; i++ {
		select {
		case msg1 := <-overviewChan:
			if msg1 != "" {
				builder.WriteString(msg1)
			}
		case msg2 := <-typeChan:
			if msg2 != "" {
				builder.WriteString(msg2)
			}
		case msg3 := <-checkChan:
			if msg3 != "" {
				builder.WriteString(msg3)
			}
		}
	}

	endTime := time.Now()
	if err := eg.Wait(); err != nil {
		service.Logger.Error("evaluate oracle migrate mysql cost finished",
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Error(fmt.Errorf("evaluate schema cost task failed, please rerunning")),
			zap.Error(err))
		return fmt.Errorf("evaluate schema cost task failed, please rerunning, error: %v", err)
	}

	if builder.String() != "" {
		pwdDir, err := os.Getwd()
		if err != nil {
			return err
		}
		file, err := os.OpenFile(filepath.Join(pwdDir, "transferdb_cost.txt"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		defer file.Close()

		service.Logger.Info("cost", zap.String("output", filepath.Join(pwdDir, "transferdb_cost.sql")))

		_, err = file.WriteString(builder.String())
		if err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		service.Logger.Info("evaluate oracle migrate mysql cost finished",
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.String("output", filepath.Join(pwdDir, "transferdb_cost.txt")))
		return nil
	}

	service.Logger.Info("evaluate oracle migrate mysql cost finished",
		zap.String("cost", endTime.Sub(startTime).String()),
		zap.String("output", "no output"))
	return nil
}

func GatherOracleOverview(schemaName []string, engine *service.Engine) (string, error) {
	var builder strings.Builder
	dbOverview, err := GatherOracleDBOverview(engine)
	if err != nil {
		return "", err
	}
	if dbOverview != "" {
		builder.WriteString(dbOverview + "\n")
	}
	schemaOverview, err := GatherOracleSchemaOverview(schemaName, engine)
	if err != nil {
		return "", err
	}
	if schemaOverview != "" {
		builder.WriteString(schemaOverview + "\n")
	}
	tableRowTop, err := GatherOracleSchemaTableRowsTOP(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableRowTop != "" {
		builder.WriteString(tableRowTop + "\n")
	}
	objectOverview, err := GatherOracleSchemaObjectOverview(schemaName, engine)
	if err != nil {
		return "", err
	}
	if objectOverview != "" {
		builder.WriteString(objectOverview + "\n")
	}
	partitionType, err := GatherOracleSchemaPartitionType(schemaName, engine)
	if err != nil {
		return "", err
	}
	if partitionType != "" {
		builder.WriteString(partitionType + "\n")
	}
	columnType, err := GatherOracleSchemaColumnTypeAndMaxLength(schemaName, engine)
	if err != nil {
		return "", err
	}
	if columnType != "" {
		builder.WriteString(columnType + "\n")
	}
	tableRow, err := GatherOracleSchemaTableAvgRowLength(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableRow != "" {
		builder.WriteString(tableRow + "\n")
	}
	temporary, err := GatherOracleSchemaTemporaryTable(schemaName, engine)
	if err != nil {
		return "", err
	}
	if temporary != "" {
		builder.WriteString(temporary + "\n")
	}
	return builder.String(), nil
}

func GatherOracleType(schemaName []string, engine *service.Engine) (string, error) {
	var builder strings.Builder
	indexType, err := GatherOracleSchemaIndexType(schemaName, engine)
	if err != nil {
		return "", err
	}
	if indexType != "" {
		builder.WriteString(indexType + "\n")
	}
	constraintType, err := GatherOracleConstraintType(schemaName, engine)
	if err != nil {
		return "", err
	}
	if constraintType != "" {
		builder.WriteString(constraintType + "\n")
	}

	codeType, err := GatherOracleSchemeCodeType(schemaName, engine)
	if err != nil {
		return "", err
	}
	if codeType != "" {
		builder.WriteString(codeType + "\n")
	}
	synonym, err := GatherOracleSchemaSynonymType(schemaName, engine)
	if err != nil {
		return "", err
	}
	if synonym != "" {
		builder.WriteString(synonym + "\n")
	}
	return builder.String(), nil
}

func GatherOracleCheck(schemaName []string, engine *service.Engine) (string, error) {
	var builer strings.Builder
	tableCounts, err := GatherOraclePartitionTableCountsCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}
	tableCounts, err = GatherOracleTableRowLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}
	tableCounts, err = GatherOracleTableIndexRowLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleTableColumnCountsCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleTableIndexCountsCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleTableNumberTypeCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleUsernameLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleTableNameLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleColumnNameLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleIndexNameLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleViewNameLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	tableCounts, err = GatherOracleSequenceNameLengthCheck(schemaName, engine)
	if err != nil {
		return "", err
	}
	if tableCounts != "" {
		builer.WriteString(tableCounts + "\n")
	}

	return builer.String(), err
}
