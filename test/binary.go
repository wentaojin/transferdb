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
package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"image"
	"image/jpeg"
	"io"
	"os"
)

func main() {
	ctx := context.Background()

	oraCfg := config.OracleConfig{
		Username:    "findpoit",
		Password:    "findpewdt",
		Host:        "10.29.123.33",
		Port:        1521,
		ServiceName: "gbk",
		Charset:     "zhs16gbk",
	}

	ora, err := oracle.NewOracleDBEngine(ctx, oraCfg, "findpt")
	if err != nil {
		panic(err)
	}

	//err = writeIMG(ora.OracleDB, "/Users/marvin/gostore/transferdb/test/20231204-113506.jpeg")
	//if err != nil {
	//	panic(err)
	//}
	//err = SaveIMG(ora.OracleDB, "/Users/marvin/gostore/transferdb/test/202312006.jpeg")
	//if err != nil {
	//	panic(err)
	//}

	//err = queryBin(ctx, ora.OracleDB)
	//if err != nil {
	//	panic(err)
	//}

	myCfg := config.MySQLConfig{
		Username: "root",
		Password: "marvin",
		Host:     "12.922.190.333",
		Port:     4000,
		Charset:  "utf8mb4",
	}

	my, err := mysql.NewMySQLDBEngine(ctx, myCfg)
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}

	err = writeDB(ctx, ora.OracleDB, my.MySQLDB)
	if err != nil {
		panic(err)
	}

	//err = SaveIMG(my.MySQLDB, "/Users/marvin/gostore/transferdb/test/202312006.jpeg")
	//if err != nil {
	//	panic(err)
	//}
}

func writeIMG(db *sql.DB, imagePath string) error {
	// Open the image file
	imageFile, err := os.Open(imagePath)
	if err != nil {
		return fmt.Errorf("Error opening image file:", err)
	}
	defer imageFile.Close()

	// Read the entire file content into a byte slice
	imageBytes, err := io.ReadAll(imageFile)
	if err != nil {
		return fmt.Errorf("Error reading image file:", err)
	}

	// Prepare the insert statement
	insertSQL := "INSERT INTO marvin_blob (id, b) VALUES (:1, :2)"

	_, err = db.Exec(insertSQL, 1, imageBytes)
	if err != nil {
		return fmt.Errorf("Error inserting image data:", err)
	}

	fmt.Printf("Image data successfully inserted into the database\n")
	return nil
}

func SaveIMG(db *sql.DB, outImgFile string) error {
	//_, res, err := oracle.Query(context.Background(), db, `SELECT b FROM marvin_blob`)
	//if err != nil {
	//	return err
	//}

	var imageBytes []byte
	rows := db.QueryRow(`SELECT b FROM marvin.marvin_blob WHERE id=1`)
	err := rows.Scan(&imageBytes)
	if err != nil {
		return err
	}

	img, _, err := image.Decode(bytes.NewReader(imageBytes))
	if err != nil {
		return err
	}
	out, _ := os.Create(outImgFile)
	defer out.Close()

	err = jpeg.Encode(out, img, nil)
	if err != nil {
		return err
	}

	fmt.Printf("Image data successfully success into the filename [%v]\n", outImgFile)
	return nil
}

func writeDB(ctx context.Context, db *sql.DB, my *sql.DB) error {
	//_, rows, err := oracle.Query(ctx, db, `SELECT id,b FROM marvin_blob WHERE ID=1`)
	//if err != nil {
	//	return err
	//}
	//for _, r := range rows {
	//	_, err = my.Exec(fmt.Sprintf("INSERT INTO marvin.marvin_blob VALUES ('%v',X'%v')", r["ID"], r["B"]))
	//	if err != nil {
	//		return err
	//	}
	//}

	rows, err := db.Query(`SELECT id,b FROM marvin_blob WHERE ID=1`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			idBytes  []byte
			blobData []byte
		)
		err = rows.Scan(&idBytes, &blobData)
		if err != nil {
			return err
		}

		_, err = my.Exec(fmt.Sprintf("INSERT INTO marvin.marvin_blob (id,b) VALUES (%v,%q)", idBytes, blobData))
		if err != nil {
			return fmt.Errorf("Error inserting BLOB data:", err)
		}
	}

	return nil
}

func queryBin(ctx context.Context, db *sql.DB) error {
	_, rs, err := oracle.Query(ctx, db, `SELECT * FROM findpt.marvin_blob where id=3`)
	if err != nil {
		return err
	}
	fmt.Println(rs)
	return nil
}
