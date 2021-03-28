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
	AppConfig       AppConfig       `toml:"app" json:"app"`
	ReverseConfig   ReverseConfig   `toml:"reverse" json:"reverse"`
	FullConfig      FullConfig      `toml:"full" json:"full"`
	IncrementConfig IncrementConfig `toml:"increment" json:"increment"`
	AllConfig       AllConfig       `toml:"all" json:"all"`
	SourceConfig    SourceConfig    `toml:"source" json:"source"`
	TargetConfig    TargetConfig    `toml:"target" json:"target"`
	LogConfig       LogConfig       `toml:"log" json:"log"`
}

type AppConfig struct {
	InsertBatchSize  int `toml:"insert-batch-size" json:"insert-batch-size"`
	SlowlogThreshold int `toml:"slowlog-threshold" json:"slowlog-threshold"`
}

type ReverseConfig struct {
	ReverseThreads int `toml:"reverse-threads" json:"reverse-threads"`
}

type FullConfig struct {
	WorkerBatch      int  `toml:"worker-batch" json:"worker-batch"`
	WorkerThreads    int  `toml:"worker-threads" json:"worker-threads"`
	TableThreads     int  `toml:"table-threads" json:"table-threads"`
	EnableCheckpoint bool `toml:"enable-checkpoint" json:"enable-checkpoint"`
}

// TODO: NOT SUPPORT
type IncrementConfig struct {
	GlobalSCN     int `toml:"global-scn" json:"global-scn"`
	WorkerThreads int `toml:"worker-threads" json:"worker-threads"`
	WorkerQueue   int `toml:"worker-queue" json:"worker-queue"`
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

func (c *CfgFile) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}
