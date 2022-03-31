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
	"time"

	"gorm.io/gorm"
)

type BaseModel struct {
	CreatedAt string `gorm:"type:timestamp;not null;comment:'创建时间'" json:"createdAt"`
	UpdatedAt string `gorm:"type:timestamp;not null;comment:'更新时间'" json:"updatedAt"`
}

func (v *BaseModel) BeforeCreate(db *gorm.DB) (err error) {
	db.Statement.SetColumn("CreatedAt", getCurrentTime())
	db.Statement.SetColumn("UpdatedAt", getCurrentTime())
	return nil
}

func (v *BaseModel) BeforeUpdate(db *gorm.DB) (err error) {
	db.Statement.SetColumn("UpdatedAt", getCurrentTime())
	return nil
}

func getCurrentTime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
