// Copyright © 2020 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package util

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

type ILogger interface {
	OpenLog()
	CloseLog()
	Log(level pipeline.LogLevel, msg string)
	Panic(err error)
}

//Default logger instance
var globalLogger jobLogger

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type jobLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	minimumLevelToLog pipeline.LogLevel // The maximum customer-desired log level for this job
	file              *os.File          // The job's log file
	logger            *log.Logger       // The Job's logger
	sanitizer         pipeline.LogSanitizer
}

func InitJobLogger(minimumLevelToLog pipeline.LogLevel) {

	globalLogger = jobLogger{
		minimumLevelToLog: minimumLevelToLog,
		sanitizer:         &azCopyLogSanitizer{},
	}

	globalLogger.OpenLog()
}

func (jl *jobLogger) OpenLog() {
	if jl.minimumLevelToLog == pipeline.LogNone {
		return
	}

	file, err := os.OpenFile(path.Join("/var/log/azcopy.log"),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644 /*Default file perm */)
	if err != nil {
		panic(err)
	}

	jl.file = file

	flags := log.LstdFlags | log.LUTC
	utcMessage := fmt.Sprintf("Log times are in UTC. Local time is " + time.Now().Format("2 Jan 2006 15:04:05"))

	jl.logger = log.New(jl.file, "", flags)
	// Log the OS Environment and OS Architecture
	jl.logger.Println("OS-Environment ", runtime.GOOS)
	jl.logger.Println("OS-Architecture ", runtime.GOARCH)
	jl.logger.Println(utcMessage)
}

func (jl *jobLogger) CloseLog() {
	if jl.minimumLevelToLog == pipeline.LogNone {
		return
	}

	jl.logger.Println("Closing Log")
	err := jl.file.Close()
	if err != nil {
		panic(err)
	}
}

func (jl jobLogger) Log(loglevel pipeline.LogLevel, msg string) {
	// If the logger for Job is not initialized i.e file is not open
	// or logger instance is not initialized, then initialize it
	/*
		if loglevel < jl.minimumLevelToLog {
			return
		}
	*/

	// ensure all secrets are redacted
	msg = jl.sanitizer.SanitizeLogMessage(msg)
	jl.logger.Println(msg)
}

func Log(logLevel pipeline.LogLevel, msg string) {
	globalLogger.Log(logLevel, msg)
}
