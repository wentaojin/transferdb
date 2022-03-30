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
	"errors"
	"log"
	"time"

	"gorm.io/gorm"
)

type BaseModel struct {
	CreatedAt string `gorm:"type:timestamp;not null;comment:'创建时间'" json:"createdAt"`
	UpdatedAt string `gorm:"type:timestamp;not null;comment:'更新时间'" json:"updatedAt"`
}

func UpdateTimeStampForCreateCallback(db *gorm.DB) {
	if db.Statement.Schema != nil {
		currentTime := getCurrentTime()
		SetSchemaFieldValue(db, "CreatedAt", currentTime)
		SetSchemaFieldValue(db, "UpdatedAt", currentTime)
	}
}

func UpdateTimeStampForUpdateCallback(db *gorm.DB) {
	// if _, ok := db.Statement.Settings.Load("gorm:update_time_stamp"); ok {
	if db.Statement.Schema != nil {
		currentTime := getCurrentTime()
		db.Statement.SetColumn("UpdatedAt", currentTime)
	}
}

func SetSchemaFieldValue(db *gorm.DB, fieldName string, value interface{}) error {
	field := db.Statement.Schema.LookUpField(fieldName)
	if field == nil {
		return errors.New("can't find the field")
	}
	err := field.Set(db.Statement.ReflectValue, value)
	if err != nil {
		log.Println("schema field set err:", err)
		return err
	}

	return nil
}

func getCurrentTime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
