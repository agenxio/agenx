package log

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"time"
)

type Priority int

const (
	LOG_EMERG Priority = iota
	LOG_ALERT
	LOG_CRIT
	LOG_ERR
	LOG_WARNING
	LOG_NOTICE
	LOG_INFO
	LOG_DEBUG
)

type logger struct {
	toSyslog          bool
	toConsole         bool
	toFile            bool
	level             Priority
	selectors         map[string]struct{}
	debugAllSelectors bool
	JSON              bool

	logger  *log.Logger
	syslog  [LOG_DEBUG + 1]*log.Logger
	rotate *Rotate
}

const stderrLogFlags = log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC | log.Lshortfile

var _log = logger{}

func LogInit(level Priority, prefix string, toSyslog bool, toConsole bool, debugSelectors []string) {
	_log.toSyslog = toSyslog
	_log.toConsole = toConsole
	_log.level = level

	_log.selectors, _log.debugAllSelectors = parseSelectors(debugSelectors)

	if _log.toSyslog {
		SetToSyslog(true, prefix)
	}

	if _log.toConsole {
		SetToConsole(true, prefix)
	}
}

func parseSelectors(selectors []string) (map[string]struct{}, bool) {
	all := false
	set := map[string]struct{}{}
	for _, selector := range selectors {
		set[selector] = struct{}{}
		if selector == "*" {
			all = true
		}
	}
	return set, all
}

func debugMessage(calldepth int, selector, format string, v ...interface{}) {
	if _log.level >= LOG_DEBUG && IsDebug(selector) {
		send(calldepth+1, LOG_DEBUG, "DBG", format, v...)
	}
}

func send(calldepth int, level Priority, prefix string, format string, v ...interface{}) {
	message := fmt.Sprintf(format, v...)
	timestamp := time.Now().Format(time.RFC3339)
	var bytes []byte
	if _log.JSON {
		log := map[string]interface{}{
			"timestamp": timestamp,
			"level":     prefix,
			"message":   message,
		}
		bytes, _ = json.Marshal(log)
	} else {
		bytes = []byte(fmt.Sprintf("%s %s %s", timestamp, prefix, message))
	}

	if _log.toSyslog {
		if _log.JSON {
			_log.syslog[level].Output(calldepth, string(bytes))
		} else {
			_log.syslog[level].Output(calldepth, string(message))
		}
	}
	if _log.toConsole {
		if _log.JSON {
			_log.logger.Output(calldepth, string(bytes))
		} else {
			_log.logger.Output(calldepth, fmt.Sprintf("%s %s", prefix, message))
		}
	}
	if _log.toFile {
		if _log.JSON {
			_log.rotate.WriteLine(bytes)
		} else {
			// Makes sure all prefixes have the same length
			prefix = prefix + strings.Repeat(" ", 4-len(prefix))
			bytes = []byte(fmt.Sprintf("%s %s %s", timestamp, prefix, message))
			_log.rotate.WriteLine(bytes)
		}
	}
}

func Debug(selector string, format string, v ...interface{}) {
	debugMessage(3, selector, format, v...)
}

func MakeDebug(selector string) func(string, ...interface{}) {
	return func(msg string, v ...interface{}) {
		debugMessage(3, selector, msg, v...)
	}
}

func IsDebug(selector string) bool {
	return _log.debugAllSelectors || HasSelector(selector)
}

func HasSelector(selector string) bool {
	_, selected := _log.selectors[selector]
	return selected
}

func msg(level Priority, prefix string, format string, v ...interface{}) {
	if _log.level >= level {
		send(4, level, prefix, format, v...)
	}
}

func Info(format string, v ...interface{}) {
	msg(LOG_INFO, "INFO", format, v...)
}

func Warn(format string, v ...interface{}) {
	msg(LOG_WARNING, "WARN", format, v...)
}

func Err(format string, v ...interface{}) {
	msg(LOG_ERR, "ERR", format, v...)
}

func Critical(format string, v ...interface{}) {
	msg(LOG_CRIT, "CRIT", format, v...)
}

func WTF(format string, v ...interface{}) {
	msg(LOG_CRIT, "CRIT", format, v)
	panic(fmt.Sprintf(format, v...))
}

func Recover(msg string) {
	if r := recover(); r != nil {
		Err("%s. Recovering, but please report this: %s.", msg, r)
		Err("Stacktrace: %s", debug.Stack())
	}
}

func SetToConsole(toConsole bool, prefix string) {
	_log.toConsole = toConsole
	if _log.toConsole {
		_log.logger = log.New(os.Stderr, prefix, stderrLogFlags)
	}
}

func SetToSyslog(toSyslog bool, prefix string) {
	_log.toSyslog = toSyslog
	if _log.toSyslog {
		for prio := LOG_EMERG; prio <= LOG_DEBUG; prio++ {
			_log.syslog[prio] = openSyslog(prio, prefix)
			if _log.syslog[prio] == nil {
				_log.toSyslog = false
				break
			}
		}
	}
}

func SetToFile(toFile bool, rotate *Rotate) error {
	if toFile {
		err := rotate.CreateDirectory()
		if err != nil {
			return err
		}
		err = rotate.CheckIfConfigSane()
		if err != nil {
			return err
		}

		_log.rotate = rotate
	}

	_log.toFile = toFile
	return nil
}