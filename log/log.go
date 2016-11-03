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

package log

import (
	"io"
	"log"
	"os"
)

// Level is the logging level.
type Level uint8

const (
	// Critical ..
	Critical Level = iota
	// Error ..
	Error
	// Warn ..
	Warn
	// Info ..
	Info
	// Debug ..
	Debug
)

var std = New(os.Stdout, "")

func getPrefix(level Level) string {
	switch level {
	case Debug:
		return "[DEBUG]"
	case Info:
		return "[INFO]"
	case Warn:
		return "[WARN]"
	case Error:
		return "[ERROR]"
	case Critical:
		return "[CRITICAL]"
	default:
		log.Panicln("Should not reach")
	}

	return ""
}

// Logger ..
type Logger struct {
	*log.Logger
	level Level
}

// New ..
func New(out io.Writer, prefix string) *Logger {
	logger := log.New(out, prefix, log.LstdFlags)
	// Default to INFO level.
	return &Logger{
		Logger: logger,
		level:  Info,
	}
}

// WithLevel set the logging level.
func (l *Logger) WithLevel(level Level) *Logger {
	l.level = level
	return l
}

// Info ..
func (l *Logger) Info(format string, args ...interface{}) {
	if l.level >= Info {
		l.Logger.Printf(getPrefix(Info)+format, args)
	}
}

// Debug ..
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level >= Debug {
		l.Logger.Printf(getPrefix(Debug)+format, args)
	}
}

// Error ..
func (l *Logger) Error(format string, args ...interface{}) {
	if l.level >= Error {
		l.Logger.Printf(getPrefix(Error)+format, args)
	}
}

// Printf ..
func Printf(format string, v ...interface{}) {
	std.Info(format, v...)
}

// Fatalf is equivalent to Printf() followed by a call to os.Exit(1).
func Fatalf(format string, v ...interface{}) {
	Printf(format, v...)
	os.Exit(1)
}

// Errorf is a wrapper for printf at error level.
func Errorf(format string, v ...interface{}) {
	std.Error(format, v...)
}
