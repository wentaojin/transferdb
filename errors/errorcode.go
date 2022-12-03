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
	DOMAIN_DB      MSErrorDomain = "DB"
	DOMAIN_CONFIG  MSErrorDomain = "CONFIG"
	DOMAIN_ASSESS  MSErrorDomain = "ASSESS"
	DOMAIN_REVERSE MSErrorDomain = "REVERSE"
	DOMAIN_CHECK   MSErrorDomain = "CHECK"

	DOMAIN_SQL_MIGRATION  MSErrorDomain = "SQL_MIGRATION"
	DOMAIN_CSV_MIGRATION  MSErrorDomain = "CSV_MIGRATION"
	DOMAIN_INCR_MIGRATION MSErrorDomain = "INCR_MIGRATION"

	DOMAIN_DATA_COMPARE MSErrorDomain = "DATA_COMPARE"
)

func (t MSErrorType) Explain() string {
	return explainMSErrorType[t]
}

func (d MSErrorDomain) Explain() string {
	return explainMSErrorDomain[d]
}

var explainMSErrorType = map[MSErrorType]string{
	TRANSFERDB: "transferdb",
}

var explainMSErrorDomain = map[MSErrorDomain]string{
	DOMAIN_DB:             "db",
	DOMAIN_CONFIG:         "config",
	DOMAIN_ASSESS:         "assess",
	DOMAIN_REVERSE:        "reverse",
	DOMAIN_CHECK:          "check",
	DOMAIN_SQL_MIGRATION:  "sql_migration",
	DOMAIN_CSV_MIGRATION:  "csv_migration",
	DOMAIN_INCR_MIGRATION: "incr_migration",
	DOMAIN_DATA_COMPARE:   "data_compare",
}
