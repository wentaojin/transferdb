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
package server

import (
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/module/prepare"
	"strings"
)

// 程序运行
func Run(ctx context.Context, cfg *config.Config) error {
	switch strings.ToUpper(strings.TrimSpace(cfg.TaskMode)) {
	case common.TaskModePrepare:
		// 表结构转换 - only prepare 阶段
		err := prepare.IPrepare(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeAssess:
		// 收集评估改造成本
		err := IAssess(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeReverse:
		// 表结构转换 - reverse 阶段
		err := IReverse(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeCheck:
		// 表结构校验 - 上下游
		err := ICheck(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeCompare:
		// 数据校验 - 以上游为准
		err := ICompare(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeCSV:
		// csv 全量数据导出
		err := ICSVer(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeFull:
		// 全量数据 ETL 非一致性（基于某个时间点，而是直接基于现有 SCN）抽取，离线环境提供与原库一致性
		err := IMigrateFull(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeAll:
		// 全量 + 增量数据同步阶段 - logminer
		err := IMigrateIncr(ctx, cfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("flag [mode] can not null or value configure error")
	}
	return nil
}
