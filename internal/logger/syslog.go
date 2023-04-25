package logger

import (
	"encoding/json"
	"fmt"

	"github.com/minio/minio/internal/logger/message/log"
)

func sendMessageToSyslog(severity LogLevel, msg string, args ...interface{}) {
	if !IsSyslog() {
		return
	}
	var message string
	if msg != "" {
		message = fmt.Sprintf(msg, args...)
	} else {
		message = fmt.Sprint(args...)
	}

	switch severity {
	case InfoLvl:
		syslogWriter.Info(message)
	case ErrorLvl:
		syslogWriter.Err(message)
	case FatalLvl:
		syslogWriter.Alert(message)
	default:
		syslogWriter.Debug(message)
	}
}

func sendErrorToSyslog(entry log.Entry) {
	entryJSON, err := json.Marshal(&entry)
	if err != nil {
		return
	}
	syslogWriter.Err("Error: " + string(entryJSON))
}
