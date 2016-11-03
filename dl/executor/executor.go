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

package executor

import (
	"fmt"
	"github.com/ClusterHQ/fli/dl/record"
	"github.com/ClusterHQ/fli/errors"
)

// Executor is an interface implemented by an object that executes the []records.
type Executor interface {
	Execute(path string, recs []record.Record) error
	EOT() bool
}

// Hdr provides common data fields and methods for all executor types.
type Hdr struct {
	eot bool
}

// EOT returns true if end of transfer is received
func (h *Hdr) EOT() bool {
	return h.eot
}

// Common executor will simply calls the shared implementation of exec() for each record
type common struct {
	Hdr
}

var (
	_ Executor = &common{}
	_ Executor = &stdout{}
	_ Executor = &safe{}
)

// NewCommonExecutor creates a new object of common executor
func NewCommonExecutor() Executor {
	return &common{}
}

// This method calls the common Exec() for each record in recs.
func (c *common) Execute(path string, recs []record.Record) error {
	for _, rec := range recs {
		if rec.Type() == record.TypeEOT {
			if c.eot {
				return errors.New("Duplicate EOT received")
			}
			c.eot = true
		}

		err := rec.Exec(path)
		if err != nil {
			return err
		}
	}

	return nil
}

// Safe executor will simply calls the shared implementation of exec() for each record
type safe struct {
	Hdr
}

// NewSafeExecutor creates a new object of safe executor
func NewSafeExecutor() Executor {
	return &safe{}
}

// This method calls the SafeExec() if it's implemented, Exec() otherwise
// for each record in recs.
func (c *safe) Execute(path string, recs []record.Record) error {
	var err error
	for _, rec := range recs {
		if rec.Type() == record.TypeEOT {
			if c.eot {
				return errors.New("Duplicate EOT received")
			}
			c.eot = true
		}

		safeRec, isSafe := rec.(record.SafeRecord)
		if isSafe {
			err = safeRec.SafeExec(path)
		} else {
			err = rec.Exec(path)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// This executor will use the shared implementation of string() for each record.
type stdout struct {
	Hdr
}

// NewStdoutExecutor creates a new object of stdout executor.
func NewStdoutExecutor() Executor {
	return &stdout{}
}

// This method prints the result of string() for each record in recs.
func (s *stdout) Execute(path string, recs []record.Record) error {
	for _, rec := range recs {
		if rec.Type() == record.TypeEOT {
			if s.eot {
				return errors.New("Duplicate EOT received")
			}
			s.eot = true
		}

		fmt.Println(rec.String())
	}

	return nil
}
