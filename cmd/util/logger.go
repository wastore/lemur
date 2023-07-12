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
	"log/syslog"
	"runtime"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

type ILogger interface {
	OpenLog()
	CloseLog()
	Log(level pipeline.LogLevel, msg string)
	Panic(err error)
	ShouldLog(level pipeline.LogLevel) bool
}

//Default logger instance
var globalLogger jobLogger

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type jobLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	minimumLevelToLog pipeline.LogLevel // The maximum customer-desired log level for this job
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

	logwriter, err := syslog.New(syslog.LOG_NOTICE, "lhsm-plugin-az")
	if err != nil {
		panic(err)
	}

	log.SetOutput(logwriter)
	flags := log.LstdFlags | log.LUTC
	utcMessage := fmt.Sprintf("Log times are in UTC. Local time is " + time.Now().Format("2 Jan 2006 15:04:05"))

	jl.logger = log.New(logwriter, "", flags)
	// Log the OS Environment and OS Architecture
	jl.logger.Println("OS-Environment ", runtime.GOOS)
	jl.logger.Println("OS-Architecture ", runtime.GOARCH)
	jl.logger.Println(utcMessage)
}

func (jl *jobLogger) CloseLog() {
	return
}

func (jl *jobLogger) Log(loglevel pipeline.LogLevel, msg string) {
	// ensure all secrets are redacted
	msg = jl.sanitizer.SanitizeLogMessage(msg)
	jl.logger.Println(msg)
}

func Log(logLevel pipeline.LogLevel, msg string) {
	if ShouldLog(logLevel) {
		globalLogger.Log(logLevel, msg)
	}
}

func ShouldLog(level pipeline.LogLevel) bool {
	return globalLogger.ShouldLog(level)
}

func (jl *jobLogger) ShouldLog(level pipeline.LogLevel) bool {
	if level == pipeline.LogNone {
		return false
	}
	return level <= jl.minimumLevelToLog
}

const tryEquals string = "Try=" // TODO: refactor so that this can be used by the retry policies too?  So that when you search the logs for Try= you are guaranteed to find both types of retry (i.e. request send retries, and body read retries)

func NewReadLogFunc(redactedURL string) func(int, error, int64, int64, bool) {

	return func(failureCount int, err error, offset int64, count int64, willRetry bool) {
		retryMessage := "Will retry"
		if !willRetry {
			retryMessage = "Will NOT retry"
		}
		globalLogger.Log(pipeline.LogInfo, fmt.Sprintf(
			"Error reading body of reply. Next try (if any) will be %s%d. %s. Error: %s. Offset: %d  Count: %d URL: %s",
			tryEquals, // so that retry wording for body-read retries is similar to that for URL-hitting retries

			// We log the number of the NEXT try, not the failure just done, so that users searching the log for "Try=2"
			// will find ALL retries, both the request send retries (which are logged as try 2 when they are made) and
			// body read retries (for which only the failure is logged - so if we did the actual failure number, there would be
			// not Try=2 in the logs if the retries work).
			failureCount+1,

			retryMessage,
			err,
			offset,
			count,
			redactedURL))
	}
}
