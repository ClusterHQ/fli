/*
 * Copyright 2016 ClusterHQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package errors

import (
	"fmt"
	"runtime"
	"strings"
)

// Error handline guidelines and usage.
// 1. Use errors.New or errors.Errorf to create the error with stack traces.
// 2. As a general guideline, if the error is returned from a function that
// is written by us ClusterHQ, we return the error directly. If it is returned
// from outside ClusterHQ code, we must handle the error by creating a new
// error using the two functions from this package in order to perserve the stack.

// MaxStackDepth since we cannot have unlimitted stack traces.
const MaxStackDepth = 50

// Error is a customized error that will get the stack.
type Error struct {
	s     string
	stack []uintptr
}

var _ error = &Error{}

// New creates a new error with the error and a stack at the time of the call.
func New(e interface{}) *Error {
	var err Error

	switch e := e.(type) {
	case error:
		err.s = e.Error()
	default:
		err.s = fmt.Sprintf("%+v", e)
	}

	stack := make([]uintptr, MaxStackDepth)
	stackLen := runtime.Callers(0, stack)
	err.stack = stack[:stackLen]
	return &err
}

// Errorf is a wrapper of New(fmt.Errorf(...))
func Errorf(format string, a ...interface{}) *Error {
	err := fmt.Errorf(format, a...)
	return New(err)
}

func (err Error) Error() string {
	return err.s
}

// GetStackTrace will turn the stored stack into hunman readable form. Each item in the
// returned slice represent one stack frame.
func (err Error) GetStackTrace() string {
	trace := make([]string, len(err.stack))
	for i, pc := range err.stack {
		f := runtime.FuncForPC(pc)
		if f == nil {
			continue
		}

		// pc - 1 because the program counters we use are usually return addresses,
		// and we want to show the line that corresponds to the function call
		file, line := f.FileLine(pc - 1)
		trace[i] = fmt.Sprintf("%s:%d (0x%x)\n", file, line, pc)
	}

	return strings.Join(trace, "")
}

// HandlerStatusError ..
type HandlerStatusError struct {
	// HTTPStatus ..
	HTTPStatus int
	// Err ..
	Err error
}

// Error satisfy the error interface.
func (hse HandlerStatusError) Error() string {
	return hse.Err.Error()
}

// Status ..
func (hse HandlerStatusError) Status() int {
	return hse.HTTPStatus
}

var _ error = &HandlerStatusError{}
