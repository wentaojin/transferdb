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
package migrate

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/transferdb/pkg/util"

	"github.com/WentaoJin/transferdb/pkg/db"

	"golang.org/x/sync/semaphore"
)

const (
	oracle    = "oracle"
	mysql     = "mysql"
	postgres  = "postgres"
	sqlServer = "sql server"
)

func ConvertOracleCreateTableSQL(convertCfg ConvertTomlCfg) {
	ora := db.NewOracleDSN(convertCfg.Source.Username,
		convertCfg.Source.Password,
		convertCfg.Source.Host,
		convertCfg.Source.Port,
		convertCfg.Source.DBName)

	switch convertCfg.Target.Datasource {
	case oracle:
		//todo
	case mysql:
		convertOracleToMysql(ora, convertCfg)
	case postgres:
		//todo
	case sqlServer:
		//todo
	}

}

/*
	Oracle Convert To Mysql
*/
func convertOracleToMysql(ora *db.Engine, convertCfg ConvertTomlCfg) {
	start := time.Now()
	schema, tableMap, viewMap := getOracleTableAndViewOperatorMap(ora, convertCfg)
	// query toml config convertCfg.Target.Schema values is equal to convert.Source.Schema
	// if not equal, show rename db name
	if convertCfg.Target.Schema != schema {
		mysql, err := db.NewMysqlDSN(convertCfg.Target.Username,
			convertCfg.Target.Password,
			convertCfg.Target.Host,
			convertCfg.Target.Port,
			convertCfg.Target.Schema)
		if err != nil {
			zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("new mysql db failed: %v", err)))
		}

		convertOracleCreateTableSQLToMysql(ora, mysql, schema, convertCfg.Target.Schema, tableMap, convertCfg)
		convertOracleCreateIndexSQLToMysql(ora, mysql, schema, convertCfg.Target.Schema, tableMap, convertCfg)
		if convertCfg.Source.ViewCreate {
			convertOracleCreateViewSQLToMysql(ora, mysql, schema, convertCfg.Target.Schema, viewMap, convertCfg)
		}

	} else {
		mysql, err := db.NewMysqlDSN(convertCfg.Target.Username,
			convertCfg.Target.Password,
			convertCfg.Target.Host,
			convertCfg.Target.Port,
			schema)
		if err != nil {
			zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("new mysql db failed: %v", err)))
		}

		convertOracleCreateTableSQLToMysql(ora, mysql, schema, schema, tableMap, convertCfg)
		convertOracleCreateIndexSQLToMysql(ora, mysql, schema, schema, tableMap, convertCfg)
		if convertCfg.Source.ViewCreate {
			convertOracleCreateViewSQLToMysql(ora, mysql, schema, schema, viewMap, convertCfg)
		}

	}
	end := time.Now()
	zlog.Logger.Info("Task finished", zap.String("msg", fmt.Sprintf("Finished convert oracle table or view To mysql\nStart Time: %s\nEnd Time: %s\nTime Consuming: %s",
		start,
		end,
		end.Sub(start))))
}

func convertOracleCreateTableSQLToMysql(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, tableMap []map[string]string, convertCfg ConvertTomlCfg) {

	if oldSchema != newSchema {
		if !db.IsExistSchemaWithInMysql(mysql, newSchema) {
			createDB := fmt.Sprintf("CREATE DATABASE %s", newSchema)
			mysql.QuerySQL(createDB)
		}
	}
	if !db.IsExistSchemaWithInMysql(mysql, oldSchema) {
		createDB := fmt.Sprintf("CREATE DATABASE %s", oldSchema)
		mysql.QuerySQL(createDB)
	}

	// turn on parallel processing
	// https://godoc.org/golang.org/x/sync/semaphore
	var coroutine int = convertCfg.Global.MaxProcs
	ctx := context.TODO()
	sem := semaphore.NewWeighted(int64(coroutine))

	for _, rowTable := range tableMap {
		if err := sem.Acquire(ctx, 1); err != nil {
			zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Error on acquire semaphore failed: %v\n", err)))
		}
		go func(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, rowTable map[string]string, convertCfg ConvertTomlCfg) {
			defer sem.Release(1)
			generateAndCreateTableInMysql(oracle, mysql, oldSchema, newSchema, rowTable, convertCfg)
		}(oracle, mysql, oldSchema, newSchema, rowTable, convertCfg)
	}

	if err := sem.Acquire(ctx, int64(coroutine)); err != nil {
		zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Error on acquire semaphore failed: %v\n", err)))
	}
}

func convertOracleCreateViewSQLToMysql(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, viewMap []map[string]string, convertCfg ConvertTomlCfg) {
	// turn on parallel processing
	// https://godoc.org/golang.org/x/sync/semaphore
	var coroutine int = convertCfg.Global.MaxProcs
	ctx := context.TODO()
	sem := semaphore.NewWeighted(int64(coroutine))

	for _, rowTable := range viewMap {
		if err := sem.Acquire(ctx, 1); err != nil {
			zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Error on acquire semaphore failed: %v\n", err)))
		}
		go func(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, rowTable map[string]string, convertCfg ConvertTomlCfg) {
			defer sem.Release(1)
			generateAndCreateViewInMysql(oracle, mysql, oldSchema, newSchema, rowTable, convertCfg)
		}(oracle, mysql, oldSchema, newSchema, rowTable, convertCfg)
	}

	if err := sem.Acquire(ctx, int64(coroutine)); err != nil {
		zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Error on acquire semaphore failed: %v\n", err)))
	}
}

func convertOracleCreateIndexSQLToMysql(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, tableMap []map[string]string, convertCfg ConvertTomlCfg) {
	// turn on parallel processing
	// https://godoc.org/golang.org/x/sync/semaphore
	var coroutine int = convertCfg.Global.MaxProcs
	ctx := context.TODO()
	sem := semaphore.NewWeighted(int64(coroutine))

	for _, rowTable := range tableMap {
		if err := sem.Acquire(ctx, 1); err != nil {
			zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Error on acquire semaphore failed: %v\n", err)))
		}
		go func(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, rowTable map[string]string, convertCfg ConvertTomlCfg) {
			defer sem.Release(1)
			generateAndCreateIndexInMysql(oracle, mysql, oldSchema, newSchema, rowTable, convertCfg)
		}(oracle, mysql, oldSchema, newSchema, rowTable, convertCfg)
	}

	if err := sem.Acquire(ctx, int64(coroutine)); err != nil {
		zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Error on acquire semaphore failed: %v\n", err)))
	}
}

func generateAndCreateTableInMysql(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, rowTable map[string]string, convertCfg ConvertTomlCfg) {
	var (
		createTableSQL string
	)
	colDefinition, colCommentSQL := oracleTableColumnMetaMapToMysql(oracle, oldSchema, newSchema, rowTable["TABLE_NAME"])
	colMeta := strings.Join(colDefinition, ",\n")
	if rowTable["COMMENTS"] != "" {
		createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin  COMMENT='%s'",
			newSchema, strings.ToLower(rowTable["TABLE_NAME"]), colMeta, rowTable["COMMENTS"])
	}
	createTableSQL = fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		newSchema, strings.ToLower(rowTable["TABLE_NAME"]), colMeta)

	// query db is exist table
	if !db.IsExistTableWithInMysql(mysql, newSchema, strings.ToLower(rowTable["TABLE_NAME"])) {
		// create table
		zlog.Logger.Info("Exec sql", zap.String("msg", fmt.Sprintf("Start exec create table %s.%s at the mysql db, Table SQL: %v", newSchema, strings.ToLower(rowTable["TABLE_NAME"]), createTableSQL)))
		mysql.QuerySQL(createTableSQL)
		// add table col comment

		for _, commentSQL := range colCommentSQL {
			if commentSQL != "" {
				zlog.Logger.Info("Exec sql", zap.String("msg", fmt.Sprintf("Start exec add table %s.%s col comment at the mysql db, Table SQL: %v",
					newSchema, strings.ToLower(rowTable["TABLE_NAME"]), commentSQL)))
				mysql.QuerySQL(commentSQL)
			}
		}
	} else {
		if convertCfg.Target.Behavior {
			// rename table
			db.RenameTableWithInMysql(mysql, newSchema, strings.ToLower(rowTable["TABLE_NAME"]))
			// create table
			zlog.Logger.Info("Exec sql", zap.String("msg", fmt.Sprintf("Start exec create table %s.%s at the mysql db, Table SQL: %v\n", newSchema,
				strings.ToLower(rowTable["TABLE_NAME"]), createTableSQL)))
			mysql.QuerySQL(createTableSQL)
		} else {
			// skip table create, and record print
			warnMsg := fmt.Sprintf("Mysql schema %s table %s exist, config file params behavior value false, table skip create",
				newSchema, strings.ToLower(rowTable["TABLE_NAME"]))
			zlog.Logger.Warn("Appear warning", zap.String("warn", warnMsg))
		}
	}
}

func generateAndCreateViewInMysql(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, rowTable map[string]string, convertCfg ConvertTomlCfg) {
	// if view exist cross schema, eg: create view scott.emp_view2 as select * from scott.emp,marvin.t2;
	// if target.schema marvin.t2 not exist,view create in the target.schema would report error,Then process exit:
	// Failed: Error 1146: Table 'marvin.t2' doesn't exist

	var (
		createViewSQL string
	)
	viewMeta := oracle.GetViewMeta(oldSchema, rowTable["TABLE_NAME"])

	for _, rowView := range viewMeta {
		if rowView["TEXT"] != "" {
			viewSQL := strings.ToLower(rowView["TEXT"])
			viewSQL = strings.Replace(viewSQL, oldSchema, newSchema, -1)

			createViewSQL = fmt.Sprintf("CREATE VIEW `%s`.`%s` AS %s", newSchema, strings.ToLower(rowTable["TABLE_NAME"]), viewSQL)
			// query db is exist table
			if !db.IsExistViewWithInMysql(mysql, newSchema, strings.ToLower(rowTable["TABLE_NAME"])) {
				// create view
				zlog.Logger.Info("Exec sql", zap.String("msg", fmt.Sprintf("Start exec create view %s.%s at the mysql db, Table SQL: %v", newSchema, strings.ToLower(rowTable["TABLE_NAME"]), createViewSQL)))
				mysql.QuerySQL(createViewSQL)
			} else {
				if convertCfg.Target.Behavior {
					// rename view
					db.RenameTableWithInMysql(mysql, newSchema, strings.ToLower(rowTable["TABLE_NAME"]))
					// create view
					zlog.Logger.Info("Exec sql", zap.String("msg", fmt.Sprintf("Start exec create view %s.%s at the mysql db, Table SQL: %v", newSchema, strings.ToLower(rowTable["TABLE_NAME"]), createViewSQL)))
					mysql.QuerySQL(createViewSQL)
				}
				// skip view create, and record print
				warnMsg := fmt.Sprintf("Mysql schema %s view %s exist, config file params behavior value false, view skip create", newSchema, strings.ToLower(rowTable["TABLE_NAME"]))
				zlog.Logger.Warn("Appear warning", zap.String("warn", warnMsg))
			}
		}
	}

}

func generateAndCreateIndexInMysql(oracle *db.Engine, mysql *db.Engine, oldSchema, newSchema string, rowTable map[string]string, convertCfg ConvertTomlCfg) {
	var (
		createIndexSQL string
	)
	idxMeta := getOracleTableIndexInfo(oracle, oldSchema, rowTable["TABLE_NAME"])
	for _, idx := range idxMeta {
		if idx["TABLE_NAME"] != "" {
			if idx["UNIQUENESS"] == "NONUNIQUE" {
				createIndexSQL = fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s`(%s)",
					strings.ToLower(idx["INDEX_NAME"]), newSchema, strings.ToLower(idx["TABLE_NAME"]), strings.ToLower(idx["COLUMN_LIST"]))
			} else {
				createIndexSQL = fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s`(%s)",
					strings.ToLower(idx["INDEX_NAME"]), newSchema, strings.ToLower(idx["TABLE_NAME"]), strings.ToLower(idx["COLUMN_LIST"]))
			}
			if !db.IsExistIndexWithInMysql(mysql, newSchema, strings.ToLower(rowTable["TABLE_NAME"]), strings.ToLower(rowTable["INDEX_NAME"])) {
				// create index
				zlog.Logger.Info("Exec sql", zap.String("msg", fmt.Sprintf("Start exec create view %s.%s at the mysql db, Table SQL: \n%v", newSchema, strings.ToLower(rowTable["TABLE_NAME"]), createIndexSQL)))
				mysql.QuerySQL(createIndexSQL)
			} else {
				// skip index create, and record print
				warnMsg := fmt.Sprintf("Mysql schema %s table %s index %s exist, index skip create", newSchema, strings.ToLower(rowTable["TABLE_NAME"]), strings.ToLower(rowTable["INDEX_NAME"]))
				zlog.Logger.Warn("Appear warning", zap.String("warn", warnMsg))
			}
		}
	}
}

func oracleTableColumnMetaMapToMysql(oracle *db.Engine, oldSchema, newSchema, table string) (colDefinition, colCommentSQL []string) {
	colMeta := getOracleTableColInfo(oracle, oldSchema, table)
	var (
		colType    string
		colInfo    string
		commentSQL string
	)

	for _, rowCol := range colMeta {
		// oracle build-in data type(20c), backward compatible
		// https://docs.oracle.com/en/database/oracle/oracle-database/20/sqlrf/Data-Types.html#GUID-A3C0D836-BADB-44E5-A5D4-265BA5968483
		// oracle convert mysql data type map https://www.convert-in.com/docs/ora2sql/types-mapping.htm
		// https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABHHAJC
		switch rowCol["DATA_TYPE"] {
		case "NUMBER":
			var (
				ds  int
				err error
			)
			if rowCol["DATA_SCALE"] == "" {
				ds = 0
			} else {
				ds, err = strconv.Atoi(fmt.Sprintf("%s", rowCol["DATA_SCALE"]))
				if err != nil {
					zlog.Logger.Fatal("Appear error", zap.String("error",
						fmt.Sprintf("Oracle DB schema %s table %s convert column data scale string to int failed: %v", newSchema, table, err)))
				}
			}
			if ds > 0 {
				colType = fmt.Sprintf("DECIMAL(%s, %s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
			} else {
				var (
					dp  int
					err error
				)
				if rowCol["DATA_PRECISION"] == "" {
					dp = 0
				} else {
					dp, err = strconv.Atoi(fmt.Sprintf("%s", rowCol["DATA_PRECISION"]))
					if err != nil {
						zlog.Logger.Fatal("Appear error", zap.String("error",
							fmt.Sprintf("Oracle DB schema %s table %s convert column data percision string to int failed: %v", newSchema, table, err)))
					}
				}
				switch {
				case dp >= 1 && dp < 3:
					colType = "TINYINT"
				case dp >= 3 && dp < 5:
					colType = "SMALLINT"
				case dp >= 5 && dp < 9:
					colType = "INT"
				case dp >= 9 && dp < 19:
					colType = "BIGINT"
				case dp >= 19 && dp <= 38:
					colType = fmt.Sprintf("DECIMAL(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
				case dp == 0 && ds == 0:
					colType = "NUMERIC"
				default:
					colType = fmt.Sprintf("DECIMAL(%s,4)", rowCol["DATA_PRECISION"])
				}
			}
			colInfo, commentSQL = getTableColMetaWithIntDefault(newSchema, table, rowCol, colType)
		case "BFILE":
			colType = "VARCHAR(255)"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "CHAR":
			if rowCol["DATA_LENGTH"] < "256" {
				colType = fmt.Sprintf("CHAR(%s)", rowCol["DATA_LENGTH"])
			}
			colType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "CHARACTER":
			if rowCol["DATA_LENGTH"] < "256" {
				colType = fmt.Sprintf("CHARACTER(%s)", rowCol["DATA_LENGTH"])
			}
			colType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "CLOB":
			colType = "LONGTEXT"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "BLOB":
			colType = "BLOB"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "DATE":
			colType = "DATETIME"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "DECIMAL":
			if rowCol["DATA_SCALE"] == "0" || rowCol["DATA_SCALE"] == "" {
				colType = fmt.Sprintf("DATETIME(%s)", rowCol["DATA_PRECISION"])
			}
			colType = fmt.Sprintf("DATETIME(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
			colInfo, commentSQL = getTableColMetaWithIntDefault(newSchema, table, rowCol, colType)
		case "DOUBLE PRECISION":
			colType = "DOUBLE PRECISION"
			colInfo, commentSQL = getTableColMetaWithFloatDefault(newSchema, table, rowCol, colType)
		case "FLOAT":
			if rowCol["DATA_PRECISION"] == "0" || rowCol["DATA_PRECISION"] == "" {
				colType = "FLOAT"
			}
			colType = "DOUBLE"
			colInfo, commentSQL = getTableColMetaWithFloatDefault(newSchema, table, rowCol, colType)
		case "INTEGER":
			colType = "INT"
			colInfo, commentSQL = getTableColMetaWithIntDefault(newSchema, table, rowCol, colType)
		case "INT":
			colType = "INT"
			colInfo, commentSQL = getTableColMetaWithIntDefault(newSchema, table, rowCol, colType)
		case "LONG":
			colType = "LONGTEXT"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "LONG RAW":
			colType = "LONGBLOB"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "BINARY_FLOAT":
			colType = "DOUBLE"
			colInfo, commentSQL = getTableColMetaWithFloatDefault(newSchema, table, rowCol, colType)
		case "BINARY_DOUBLE":
			colType = "DOUBLE"
			colInfo, commentSQL = getTableColMetaWithFloatDefault(newSchema, table, rowCol, colType)
		case "NCHAR":
			if rowCol["DATA_LENGTH"] < "256" {
				colType = fmt.Sprintf("NCHAR(%s)", rowCol["DATA_LENGTH"])
			}
			colType = fmt.Sprintf("NVARCHAR(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "NCHAR VARYING":
			colType = fmt.Sprintf("NCHAR VARYING(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "NCLOB":
			colType = "NVARCHAR(max)"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "NUMERIC":
			colType = fmt.Sprintf("NUMERIC(%s,%s)", rowCol["DATA_PRECISION"], rowCol["DATA_SCALE"])
			colInfo, commentSQL = getTableColMetaWithIntDefault(newSchema, table, rowCol, colType)
		case "NVARCHAR2":
			colType = fmt.Sprintf("NVARCHAR(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "RAW":
			if rowCol["DATA_LENGTH"] < "256" {
				colType = fmt.Sprintf("BINARY(%s)", rowCol["DATA_LENGTH"])
			}
			colType = fmt.Sprintf("VARBINARY(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "real":
			colType = "DOUBLE"
			colInfo, commentSQL = getTableColMetaWithFloatDefault(newSchema, table, rowCol, colType)
		case "ROWID":
			colType = "CHAR(10)"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "SMALLINT":
			colType = "DECIMAL(38)"
			colInfo, commentSQL = getTableColMetaWithIntDefault(newSchema, table, rowCol, colType)
		case "UROWID":
			colType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "VARCHAR2":
			colType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "VARCHAR":
			colType = fmt.Sprintf("VARCHAR(%s)", rowCol["DATA_LENGTH"])
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		case "XMLTYPE":
			colType = "LONGTEXT"
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		default:
			if strings.Contains(rowCol["DATA_TYPE"], "INTERVAL") {
				colType = "VARCHAR(30)"
			} else if strings.Contains(rowCol["DATA_TYPE"], "TIMESTAMP") {
				if rowCol["DATA_SCALE"] == "" {
					colType = "DATETIME"
				}
				colType = fmt.Sprintf("DATETIME(%s)", rowCol["DATA_SCALE"])
			} else {
				colType = "TEXT"
			}
			colInfo, commentSQL = getTableColMetaWithStringDefault(newSchema, table, rowCol, colType)
		}
		colCommentSQL = append(colCommentSQL, commentSQL)
		colDefinition = append(colDefinition, colInfo)
	}

	// oracle table primary key、unique key、foreign key
	colDefinition = getOracleTableKeyInfo(oracle, oldSchema, table, colDefinition)
	return
}

func getOracleTableKeyInfo(oracle *db.Engine, schema, table string, colDefinition []string) []string {
	var (
		pkMeta, ukMeta, fkMeta []map[string]string
		pkInfo                 string
		ukInfo                 []string
		fkInfo                 []string
	)
	pkMeta = oracle.GetTablePrimaryKey(schema, table)
	ukMeta = oracle.GetTableUniqueKey(schema, table)
	fkMeta = oracle.GetTableForeignKey(schema, table)
	if len(pkMeta) > 1 {
		zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Oracle DB schema %s table %s primary key exist multiple values: %v", schema, table, pkMeta)))
	}
	if len(pkMeta) != 0 {
		pkInfo = fmt.Sprintf("PRIMARY KEY (%s)", strings.ToLower(pkMeta[0]["COLUMN_LIST"]))
		colDefinition = append(colDefinition, pkInfo)

	}
	if len(ukMeta) != 0 {
		for _, rowUKCol := range ukMeta {
			uk := fmt.Sprintf("UNIQUE KEY `%s` (%s)", strings.ToLower(rowUKCol["CONSTRAINT_NAME"]), strings.ToLower(rowUKCol["COLUMN_LIST"]))
			ukInfo = append(ukInfo, uk)
		}
		colDefinition = append(colDefinition, ukInfo...)
	}
	if len(fkMeta) != 0 {
		for _, rowFKCol := range fkMeta {
			fk := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(`%s`) REFERENCES `%s`(`%s`)",
				strings.ToLower(rowFKCol["CONSTRAINT_NAME"]),
				strings.ToLower(rowFKCol["COLUMN_NAME"]),
				strings.ToLower(rowFKCol["RTABLE_NAME"]),
				strings.ToLower(rowFKCol["RCOLUMN_NAME"]))
			fkInfo = append(fkInfo, fk)
		}
		colDefinition = append(colDefinition, fkInfo...)
	}
	return colDefinition
}

func getTableColMetaWithStringDefault(schema, table string, rowCol map[string]string, colType string) (colInfo string, commentSQL string) {
	var nullable string
	if rowCol["NULLABLE"] == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}
	if rowCol["DATA_DEFAULT"] != "" {
		colInfo = fmt.Sprintf("`%s` %s %s DEFAULT %s", strings.ToLower(rowCol["COLUMN_NAME"]), colType, nullable, rowCol["DATA_DEFAULT"])
		if rowCol["COMMENTS"] != "" {
			commentSQL = fmt.Sprintf("Alter table `%s`.`%s`  MODIFY COLUMN %s Comment '%s'", schema, strings.ToLower(table), colInfo, rowCol["COMMENTS"])
		}
	}
	colInfo = fmt.Sprintf("`%s` %s %s", strings.ToLower(rowCol["COLUMN_NAME"]), colType, nullable)
	return
}

func getTableColMetaWithIntDefault(schema, table string, rowCol map[string]string, colType string) (colInfo string, commentSQL string) {
	var nullable string
	if rowCol["NULLABLE"] == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}
	if rowCol["DATA_DEFAULT"] != "" {
		defaultIntValue, _ := strconv.Atoi(rowCol["DATA_DEFAULT"])
		colInfo = fmt.Sprintf("`%s` %s %s DEFAULT %d", strings.ToLower(rowCol["COLUMN_NAME"]), colType, nullable, defaultIntValue)
		if rowCol["COMMENTS"] != "" {
			commentSQL = fmt.Sprintf("Alter table `%s`.`%s`  MODIFY COLUMN %s Comment '%s'", schema, strings.ToLower(table), colInfo, rowCol["COMMENTS"])
		}
	}
	colInfo = fmt.Sprintf("`%s` %s %s", strings.ToLower(rowCol["COLUMN_NAME"]), colType, nullable)
	return
}

func getTableColMetaWithFloatDefault(schema, table string, rowCol map[string]string, colType string) (colInfo string, commentSQL string) {
	var nullable string
	if rowCol["NULLABLE"] == "Y" {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}
	if rowCol["DATA_DEFAULT"] != "" {
		defaultFloatValue, _ := strconv.ParseFloat(rowCol["DATA_DEFAULT"], 64)
		colInfo = fmt.Sprintf("`%s` %s %s DEFAULT %f", strings.ToLower(rowCol["COLUMN_NAME"]), colType, nullable, defaultFloatValue)
		if rowCol["COMMENTS"] != "" {
			commentSQL = fmt.Sprintf("Alter table `%s`.`%s`  MODIFY COLUMN %s Comment '%s'", schema, strings.ToLower(table), colInfo, rowCol["COMMENTS"])
		}
	}
	colInfo = fmt.Sprintf("`%s` %s %s", strings.ToLower(rowCol["COLUMN_NAME"]), colType, nullable)

	return
}

/*
	Common
*/
func getOracleTableAndViewOperatorMap(oracle *db.Engine, convertCfg ConvertTomlCfg) (schema string, tableMap, viewMap []map[string]string) {
	var (
		isContainer bool
	)
	isContainer, schema = containerOracleSchema(oracle)
	if !isContainer {
		zlog.Logger.Fatal("Appear error", zap.String("error", fmt.Sprintf("Error on toml config file source params schema value isn't found in the oracle schema list")))

	}

	tableMeta := getOracleSchemaTableMeta(oracle, schema, convertCfg.Source.IncludeTable, convertCfg.Source.ExcludeTable)

	for _, rowTable := range tableMeta {
		if rowTable["TABLE_TYPE"] == "TABLE" {
			tableMap = append(tableMap, rowTable)
		} else if rowTable["TABLE_TYPE"] == "VIEW" {
			viewMap = append(viewMap, rowTable)
		} else {
			// at this stage, it is not supported except for the following table types and view types.
			zlog.Logger.Warn("Not support", zap.String("warn", fmt.Sprintf("At this stage, type %s is not supported except for the following table types and view types.", rowTable["TABLE_TYPE"])))
		}
	}

	return
}

func getOracleSchemaTableMeta(oracle *db.Engine, schema string, includeTable, excludeTable []string) []map[string]string {
	var (
		originTableList                      []string
		includeUpperTable, excludeUpperTable []string
		finallyTableMeta                     []map[string]string
	)
	tableMeta := oracle.GetTableMeta(schema)

	for _, t := range tableMeta {
		originTableList = append(originTableList, t["TABLE_NAME"])
	}

	for _, table := range includeTable {
		includeUpperTable = append(includeUpperTable, strings.ToUpper(table))
	}

	for _, table := range excludeTable {
		excludeUpperTable = append(excludeUpperTable, strings.ToUpper(table))
	}

	switch {
	case len(includeUpperTable) == 0 && len(excludeUpperTable) == 0:
		return tableMeta
	case len(includeUpperTable) != 0 && len(excludeUpperTable) == 0:
		repeatElem := util.FindStringSliceRepeatElem(includeUpperTable, originTableList)
		diffElem := util.FindStringSlicesDiffElem(repeatElem, originTableList)
		if len(diffElem) != 0 {
			zlog.Logger.Error("Appear error", zap.String("error", fmt.Sprintf("Error on toml config file source params includeTable value %v Not exist in the database schema", diffElem)))
		}
		for _, table := range includeUpperTable {
			for _, t := range tableMeta {
				if table == t["TABLE_NAME"] {
					finallyTableMeta = append(finallyTableMeta, t)
				}
			}
		}
		return finallyTableMeta
	case len(includeUpperTable) == 0 && len(excludeUpperTable) != 0:
		repeatElem := util.FindStringSliceRepeatElem(excludeUpperTable, originTableList)
		diffElem := util.FindStringSlicesDiffElem(repeatElem, originTableList)
		if len(diffElem) != 0 {
			zlog.Logger.Error("Appear error", zap.String("error", fmt.Sprintf("Error on toml config file source params excludeTable value %v Not exist in the database schema", diffElem)))
		}
		tableList := util.FindStringSlicesDiffElem(excludeUpperTable, originTableList)
		for _, table := range tableList {
			for _, t := range tableMeta {
				if table == t["TABLE_NAME"] {
					finallyTableMeta = append(finallyTableMeta, t)
				}
			}
		}
		return finallyTableMeta
	default:
		repeatElem := util.FindStringSliceRepeatElem(includeUpperTable, originTableList)
		diffElem := util.FindStringSlicesDiffElem(repeatElem, originTableList)
		if len(diffElem) != 0 {
			zlog.Logger.Error("Appear error", zap.String("error", fmt.Sprintf("Error on toml config file source params includeTable value %v Not exist in the database schema", diffElem)))
		}
		for _, table := range includeUpperTable {
			for _, t := range tableMeta {
				if table == t["TABLE_NAME"] {
					finallyTableMeta = append(finallyTableMeta, t)
				}
			}
		}
		return finallyTableMeta
	}
}

func getOracleTableColInfo(oracle *db.Engine, schema, table string) (colMeta []map[string]string) {
	colMeta = oracle.GetTableColumnMeta(schema, table)
	return
}

func getOracleTableIndexInfo(oracle *db.Engine, schema, table string) (idxMeta []map[string]string) {
	idxMeta = oracle.GetTableIndexMeta(schema, table)
	return
}

func containerOracleSchema(oracle *db.Engine) (bool, string) {
	schema := MustGetTomlString("source.schema")
	schemaList := oracle.GetSchemaMeta()
	if !util.Contain(strings.ToUpper(schema), schemaList) {
		return false, ""
	}
	return true, schema
}
