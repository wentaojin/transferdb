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
package errors

import (
	"fmt"
)

// MSError returns business error
type MSError struct {
	errType   MSErrorType
	errDomain MSErrorDomain
	cause     error
}

func NewMSError(errType MSErrorType, errDomain MSErrorDomain, err error) MSError {
	return MSError{
		errType:   errType,
		errDomain: errDomain,
		cause:     err,
	}
}

func (e MSError) Error() string {
	if e.cause == nil {
		return ""
	}

	errInfo := fmt.Sprintf("[TYPE] %s", e.GetCodeText())
	if len(e.errDomain) > 0 {
		errInfo = fmt.Sprintf("%s [DOMAIN]: %s", errInfo, e.GetErrDomain())
	}
	errInfo = fmt.Sprintf("%s [CAUSE]: %s", errInfo, e.cause.Error())

	return errInfo
}

func (e MSError) GetCodeText() string {
	return e.errType.Explain()
}

func (e MSError) GetErrDomain() string {
	return e.errDomain.Explain()
}
