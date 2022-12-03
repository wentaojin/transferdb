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
package prepare

import (
	"context"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/module/engine"
	"go.uber.org/zap"
	"time"
)

func TPrepare(ctx context.Context, cfg *config.Config) error {
	startTime := time.Now()
	zap.L().Info("prepare tansferdb env start")
	metaDB, err := engine.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return err
	}

	err = metaDB.MigrateTables()
	if err != nil {
		return err
	}
	err = metaDB.InitDefaultValue(ctx)
	if err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("prepare tansferdb env finished", zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
