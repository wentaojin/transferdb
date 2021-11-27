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
package service

import (
	"encoding/json"
	"fmt"

	"github.com/BurntSushi/toml"
)

// 程序配置文件
type CfgFile struct {
	AppConfig    AppConfig    `toml:"app" json:"app"`
	FullConfig   FullConfig   `toml:"full" json:"full"`
	AllConfig    AllConfig    `toml:"all" json:"all"`
	SourceConfig SourceConfig `toml:"source" json:"source"`
	TargetConfig TargetConfig `toml:"target" json:"target"`
	LogConfig    LogConfig    `toml:"log" json:"log"`
}

type AppConfig struct {
	InsertBatchSize  int    `toml:"insert-batch-size" json:"insert-batch-size"`
	SlowlogThreshold int    `toml:"slowlog-threshold" json:"slowlog-threshold"`
	Threads          int    `toml:"threads" json:"threads"`
	PprofPort        string `toml:"pprof-port" json:"pprof-port"`
}

type FullConfig struct {
	WorkerBatch      int  `toml:"worker-batch" json:"worker-batch"`
	WorkerThreads    int  `toml:"worker-threads" json:"worker-threads"`
	TableThreads     int  `toml:"table-threads" json:"table-threads"`
	EnableCheckpoint bool `toml:"enable-checkpoint" json:"enable-checkpoint"`
}

type AllConfig struct {
	LogminerQueryTimeout int `toml:"logminer-query-timeout" json:"logminer-query-timeout"`
	FilterThreads        int `toml:"filter-threads" json:"filter-threads"`
	ApplyThreads         int `toml:"apply-threads" json:"apply-threads"`
	WorkerQueue          int `toml:"worker-queue" json:"worker-queue"`
	WorkerThreads        int `toml:"worker-threads" json:"worker-threads"`
}

type SourceConfig struct {
	Username      string   `toml:"username" json:"username"`
	Password      string   `toml:"password" json:"password"`
	ConnectString string   `toml:"connect-string",json:"connect-string"`
	SessionParams []string `toml:"session-params" json:"session-params"`
	Timezone      string   `toml:"timezone" json:"timezone"`
	SchemaName    string   `toml:"schema-name",json:"schema-name"`
	IncludeTable  []string `toml:"include-table",json:"include-table"`
	ExcludeTable  []string `toml:"exclude-table",json:"exclude-table"`
}

type TargetConfig struct {
	Username      string `toml:"username" json:"username"`
	Password      string `toml:"password" json:"password"`
	Host          string `toml:"host" json:"host"`
	Port          int    `toml:"port" json:"port"`
	ConnectParams string `toml:"connect-params" json:"connect-params"`
	MetaSchema    string `toml:"meta-schema" json:"meta-schema"`
	SchemaName    string `toml:"schema-name",json:"schema-name"`
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

// 根据配置文件获取表列表
func (c *CfgFile) GenerateTables(engine *Engine) ([]string, error) {
	var (
		exporterTableSlice, upperTableSlice []string
		err                                 error
	)
	switch {
	case len(c.SourceConfig.IncludeTable) != 0 && len(c.SourceConfig.ExcludeTable) == 0:
		if err := engine.IsExistOracleTable(c.SourceConfig.SchemaName, c.SourceConfig.IncludeTable); err != nil {
			return exporterTableSlice, err
		}
		exporterTableSlice = append(exporterTableSlice, c.SourceConfig.IncludeTable...)
	case len(c.SourceConfig.IncludeTable) == 0 && len(c.SourceConfig.ExcludeTable) != 0:
		exporterTableSlice, err = engine.FilterDifferenceOracleTable(c.SourceConfig.SchemaName, c.SourceConfig.ExcludeTable)
		if err != nil {
			return exporterTableSlice, err
		}
	case len(c.SourceConfig.IncludeTable) == 0 && len(c.SourceConfig.ExcludeTable) == 0:
		exporterTableSlice, err = engine.GetOracleTable(c.SourceConfig.SchemaName)
		if err != nil {
			return exporterTableSlice, err
		}
	default:
		return exporterTableSlice, fmt.Errorf("source config params include-table/exclude-table cannot exist at the same time")
	}

	if len(exporterTableSlice) == 0 {
		return exporterTableSlice, fmt.Errorf("exporter table slice can not null from reverse task")
	}
	return upperTableSlice, nil
}

func (c *CfgFile) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}
