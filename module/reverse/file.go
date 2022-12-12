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
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"os"
	"sync"
)

type File struct {
	RFile   *os.File
	CFile   *os.File
	RWriter *bufio.Writer
	CWriter *bufio.Writer
	Mutex   *sync.Mutex

	MySQL  *mysql.MySQL
	Oracle *oracle.Oracle
}

func NewWriter(reverseFile, compFile string, mysql *mysql.MySQL, oracle *oracle.Oracle) (*File, error) {
	f := &File{}
	err := f.initOutFile(reverseFile, compFile)
	if err != nil {
		return nil, err
	}

	f.Mutex = &sync.Mutex{}
	f.MySQL = mysql
	f.Oracle = oracle
	return f, nil
}

func (f *File) RWriteString(s string) (nn int, err error) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	return f.RWriter.WriteString(s)
}

func (f *File) CWriteString(s string) (nn int, err error) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	return f.CWriter.WriteString(s)
}

func (f *File) initOutFile(reverseFile, compFile string) error {
	outReverseFile, err := os.OpenFile(reverseFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	outCompFile, err := os.OpenFile(compFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	f.RWriter, f.RFile = bufio.NewWriter(outReverseFile), outReverseFile
	f.CWriter, f.CFile = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (f *File) Close() error {
	if f.RFile != nil {
		err := f.RWriter.Flush()
		if err != nil {
			return err
		}
		err = f.RFile.Close()
		if err != nil {
			return err
		}
	}
	if f.CFile != nil {
		err := f.CWriter.Flush()
		if err != nil {
			return err
		}
		err = f.CFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
