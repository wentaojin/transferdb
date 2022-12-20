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
package errors

import "github.com/wentaojin/transferdb/common"

type (
	MSErrorType   string
	MSErrorDomain string
)

// program error type
const (
	TRANSFERDB MSErrorType = "TRANSFERDB"
)

// program error domain
const (
	DOMAIN_CONFIG  MSErrorDomain = common.TaskTypeConfig
	DOMAIN_DB      MSErrorDomain = common.TaskTypeDatabase
	DOMAIN_ASSESS  MSErrorDomain = common.TaskTypeObjectAssess
	DOMAIN_REVERSE MSErrorDomain = common.TaskTypeObjectReverse
	DOMAIN_CHECK   MSErrorDomain = common.TaskTypeObjectCheck

	DOMAIN_SQL_MIGRATION  MSErrorDomain = common.TaskTypeDataSQLMigrate
	DOMAIN_CSV_MIGRATION  MSErrorDomain = common.TaskTypeDataCSVMigrate
	DOMAIN_INCR_MIGRATION MSErrorDomain = common.TaskTypeDataIncrMigrate

	DOMAIN_DATA_COMPARE MSErrorDomain = common.TaskTypeDataCompare
)

func (t MSErrorType) Explain() string {
	return explainMSErrorType[t]
}

func (d MSErrorDomain) Explain() string {
	return explainMSErrorDomain[d]
}

var explainMSErrorType = map[MSErrorType]string{
	TRANSFERDB: "TRANSFERDB",
}

var explainMSErrorDomain = map[MSErrorDomain]string{
	DOMAIN_CONFIG:         common.TaskTypeConfig,
	DOMAIN_DB:             common.TaskTypeDatabase,
	DOMAIN_ASSESS:         common.TaskTypeObjectAssess,
	DOMAIN_REVERSE:        common.TaskTypeObjectReverse,
	DOMAIN_CHECK:          common.TaskTypeObjectCheck,
	DOMAIN_SQL_MIGRATION:  common.TaskTypeDataSQLMigrate,
	DOMAIN_CSV_MIGRATION:  common.TaskTypeDataCSVMigrate,
	DOMAIN_INCR_MIGRATION: common.TaskTypeDataIncrMigrate,
	DOMAIN_DATA_COMPARE:   common.TaskTypeDataCompare,
}
