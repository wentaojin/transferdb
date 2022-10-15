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
package config

import (
	"encoding/json"
	"fmt"

	"github.com/BurntSushi/toml"
)

// 程序配置文件
type CfgFile struct {
	AppConfig    AppConfig    `toml:"app" json:"app"`
	FullConfig   FullConfig   `toml:"full" json:"full"`
	CSVConfig    CSVConfig    `toml:"csv" json:"csv"`
	AllConfig    AllConfig    `toml:"all" json:"all"`
	OracleConfig OracleConfig `toml:"oracle" json:"oracle"`
	MySQLConfig  MySQLConfig  `toml:"mysql" json:"mysql"`
	LogConfig    LogConfig    `toml:"log" json:"log"`
	DiffConfig   DiffConfig   `toml:"diff" json:"diff"`
}

type AppConfig struct {
	InsertBatchSize  int    `toml:"insert-batch-size" json:"insert-batch-size"`
	SlowlogThreshold int    `toml:"slowlog-threshold" json:"slowlog-threshold"`
	Threads          int    `toml:"threads" json:"threads"`
	PprofPort        string `toml:"pprof-port" json:"pprof-port"`
}

type DiffConfig struct {
	ChunkSize         int           `toml:"chunk-size" json:"chunk-size"`
	DiffThreads       int           `toml:"diff-threads" json:"diff-threads"`
	OnlyCheckRows     bool          `toml:"only-check-rows" json:"only-check-rows"`
	EnableCheckpoint  bool          `toml:"enable-checkpoint" json:"enable-checkpoint"`
	IgnoreStructCheck bool          `toml:"ignore-struct-check" json:"ignore-struct-check"`
	FixSqlFile        string        `toml:"fix-sql-file" json:"fix-sql-file"`
	TableConfig       []TableConfig `toml:"table-config" json:"table-config"`
}

type TableConfig struct {
	SourceTable string `toml:"source-table" json:"source-table"`
	IndexFields string `toml:"index-fields" json:"index-fields"`
	Range       string `toml:"range" json:"range"`
}

type CSVConfig struct {
	Header           bool   `toml:"header" json:"header"`
	Separator        string `toml:"separator" json:"separator"`
	Terminator       string `toml:"terminator" json:"terminator"`
	Delimiter        string `toml:"delimiter" json:"delimiter"`
	EscapeBackslash  bool   `toml:"escape-backslash" json:"escape-backslash"`
	Charset          string `toml:"charset" json:"charset"`
	Rows             int    `toml:"rows" json:"rows"`
	OutputDir        string `toml:"output-dir" json:"output-dir"`
	TaskThreads      int    `toml:"task-threads" json:"task-threads"`
	TableThreads     int    `toml:"table-threads" json:"table-threads"`
	SQLThreads       int    `toml:"sql-threads" json:"sql-threads"`
	EnableCheckpoint bool   `toml:"enable-checkpoint" json:"enable-checkpoint"`
}

type FullConfig struct {
	ChunkSize        int  `toml:"chunk-size" json:"chunk-size"`
	TaskThreads      int  `toml:"task-threads" json:"task-threads"`
	TableThreads     int  `toml:"table-threads" json:"table-threads"`
	SQLThreads       int  `toml:"sql-threads" json:"sql-threads"`
	ApplyThreads     int  `toml:"apply-threads" json:"apply-threads"`
	EnableCheckpoint bool `toml:"enable-checkpoint" json:"enable-checkpoint"`
}

type AllConfig struct {
	LogminerQueryTimeout int `toml:"logminer-query-timeout" json:"logminer-query-timeout"`
	FilterThreads        int `toml:"filter-threads" json:"filter-threads"`
	ApplyThreads         int `toml:"apply-threads" json:"apply-threads"`
	WorkerQueue          int `toml:"worker-queue" json:"worker-queue"`
	WorkerThreads        int `toml:"worker-threads" json:"worker-threads"`
}

type OracleConfig struct {
	OraArch       string   `toml:"ora-arch" json:"ora-arch"`
	Username      string   `toml:"username" json:"username"`
	Password      string   `toml:"password" json:"password"`
	Host          string   `toml:"host" json:"host"`
	Port          int      `toml:"port" json:"port"`
	ServiceName   string   `toml:"service-name" json:"service-name"`
	LibDir        string   `toml:"lib-dir" json:"lib-dir"`
	ConnectParams string   `toml:"connect-params" json:"connect-params"`
	SessionParams []string `toml:"session-params" json:"session-params"`
	SchemaName    string   `toml:"schema-name" json:"schema-name"`
	IncludeTable  []string `toml:"include-table" json:"include-table"`
	ExcludeTable  []string `toml:"exclude-table" json:"exclude-table"`
}

type MySQLConfig struct {
	DBType        string `toml:"db-type" json:"db-type"`
	Username      string `toml:"username" json:"username"`
	Password      string `toml:"password" json:"password"`
	Host          string `toml:"host" json:"host"`
	Port          int    `toml:"port" json:"port"`
	ConnectParams string `toml:"connect-params" json:"connect-params"`
	MetaSchema    string `toml:"meta-schema" json:"meta-schema"`
	SchemaName    string `toml:"schema-name" json:"schema-name"`
	TableOption   string `toml:"table-option" json:"table-option"`
	Overwrite     bool   `toml:"overwrite" json:"overwrite"`
}

type LogConfig struct {
	LogLevel   string `toml:"log-level" json:"log-level"`
	LogFile    string `toml:"log-file" json:"log-file"`
	MaxSize    int    `toml:"max-size" json:"max-size"`
	MaxDays    int    `toml:"max-days" json:"max-days"`
	MaxBackups int    `toml:"max-backups" json:"max-backups"`
}

// 读取配置文件
func ReadConfigFile(file string) (*CfgFile, error) {
	cfg := &CfgFile{}
	if err := cfg.configFromFile(file); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// 加载配置文件并解析
func (c *CfgFile) configFromFile(file string) error {
	if _, err := toml.DecodeFile(file, c); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	return nil
}

func (c *CfgFile) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}
