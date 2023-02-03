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
package check

import (
	"bufio"
	"os"
	"sync"
)

type File struct {
	CFile   *os.File
	CWriter *bufio.Writer
	Mutex   *sync.Mutex
}

func NewWriter(checkFile string) (*File, error) {
	f := &File{}
	err := f.initOutFile(checkFile)
	if err != nil {
		return nil, err
	}

	f.Mutex = &sync.Mutex{}
	return f, nil
}

func (f *File) CWriteFile(s string) (nn int, err error) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	return f.CWriter.WriteString(s)
}

func (f *File) initOutFile(checkFile string) error {
	outCheckFile, err := os.OpenFile(checkFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	f.CWriter, f.CFile = bufio.NewWriter(outCheckFile), outCheckFile
	return nil
}

func (f *File) Close() error {
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
