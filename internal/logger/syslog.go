// Copyright (c) 2022-2023 Panasas, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
