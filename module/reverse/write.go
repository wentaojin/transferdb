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
package reverse

import (
	"bufio"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"os"
	"strings"
	"sync"
)

type Write struct {
	RFile   *os.File
	CFile   *os.File
	RWriter *bufio.Writer
	CWriter *bufio.Writer
	Mutex   *sync.Mutex

	Mode        string
	DirectWrite bool
	MySQL       *mysql.MySQL
	Oracle      *oracle.Oracle
}

func NewWriter(reverseFile, compFile, mode string, directWrite bool, mysql *mysql.MySQL, oracle *oracle.Oracle) (*Write, error) {
	w := &Write{}
	if !directWrite {
		err := w.initOutReverseFile(reverseFile)
		if err != nil {
			return nil, err
		}
	}
	err := w.initOutCompatibleFile(compFile)
	if err != nil {
		return nil, err
	}
	w.Mode = mode
	w.Mutex = &sync.Mutex{}
	w.DirectWrite = directWrite
	w.MySQL = mysql
	w.Oracle = oracle
	return w, nil
}

func (w *Write) RWriteFile(s string) (nn int, err error) {
	w.Mutex.Lock()
	defer w.Mutex.Unlock()
	return w.RWriter.WriteString(s)
}

func (w *Write) RWriteDB(s string) error {
	if strings.EqualFold(w.Mode, common.ReverseO2MMode) {
		err := w.MySQL.WriteMySQLTable(s)
		if err != nil {
			return err
		}
	}
	if strings.EqualFold(w.Mode, common.ReverseM2OMode) {
		err := w.Oracle.WriteOracleTable(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Write) CWriteFile(s string) (nn int, err error) {
	w.Mutex.Lock()
	defer w.Mutex.Unlock()
	return w.CWriter.WriteString(s)
}

func (w *Write) initOutReverseFile(reverseFile string) error {
	outReverseFile, err := os.OpenFile(reverseFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	w.RWriter, w.RFile = bufio.NewWriter(outReverseFile), outReverseFile
	return nil
}

func (w *Write) initOutCompatibleFile(compFile string) error {
	outCompFile, err := os.OpenFile(compFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	w.CWriter, w.CFile = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (w *Write) Close() error {
	if w.RFile != nil {
		err := w.RWriter.Flush()
		if err != nil {
			return err
		}
		err = w.RFile.Close()
		if err != nil {
			return err
		}
	}
	if w.CFile != nil {
		err := w.CWriter.Flush()
		if err != nil {
			return err
		}
		err = w.CFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
