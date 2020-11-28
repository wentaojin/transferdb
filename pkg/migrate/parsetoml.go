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
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/WentaoJin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/spf13/viper"
)

type ConvertTomlCfg struct {
	Source Source `toml:"source"`
	Target Target `toml:"target"`
	Global Global `toml:"global"`
}

type Source struct {
	Datasource   string   `toml:"datasource"`
	Host         string   `toml:"host"`
	Username     string   `toml:"username"`
	Password     string   `toml:"password"`
	Port         string   `toml:"port"`
	DBName       string   `toml:"dbName"`
	Schema       string   `toml:"schema"`
	IncludeTable []string `toml:"includeTable"`
	ExcludeTable []string `toml:"excludeTable"`
	ViewCreate   bool     `toml:"viewCreate"`
}
type Target struct {
	Datasource string `toml:"datasource"`
	Host       string `toml:"host"`
	Username   string `toml:"username"`
	Password   string `toml:"password"`
	Port       string `toml:"port"`
	DBName     string `toml:"dbName"`
	Schema     string `toml:"schema"`
	Behavior   bool   `toml:"behavior"`
}

type Global struct {
	MaxProcs int `toml:"MaxProcs"`
}

func NewConvertCmdCfg(fileName string) (ConvertTomlCfg, error) {
	var cfg ConvertTomlCfg
	splits := strings.Split(filepath.Base(fileName), ".")
	viper.SetConfigName(filepath.Base(splits[0]))
	viper.AddConfigPath(filepath.Dir(fileName))
	err := viper.ReadInConfig()
	if err != nil {
		return cfg, fmt.Errorf("error on viper.ReadInConfig failed: %v", err)
	}
	// bind the configuration to a structure, config file display with json style

	err = viper.Unmarshal(&cfg)
	if err != nil {
		return cfg, fmt.Errorf("error on viper.Unmarshal failed: %v", err)
	}
	jsIndent, _ := json.MarshalIndent(&cfg, "", "\t")
	zlog.Logger.Info("command convert read toml config file, display config file with json style", zap.String("config", string(jsIndent)))
	return cfg, nil
}

func MustGetTomlString(key string) string {
	checkKey(key)
	return viper.GetString(key)
}

func MustGetTomlStringSlice(key string) []string {
	checkKey(key)
	return viper.GetStringSlice(key)
}

func MustGetTomlInt(key string) int {
	checkKey(key)
	return viper.GetInt(key)
}

func MustGetTomlBool(key string) bool {
	checkKey(key)
	return viper.GetBool(key)
}

func GetTomlString(key string) string {
	return viper.GetString(key)
}

func checkKey(key string) {
	if !viper.IsSet(key) {
		zlog.Logger.Fatal("Configuration key not found; aborting", zap.String("key", key))
	}
}
