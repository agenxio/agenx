package log

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/queueio/sentry/utils/paths"
)

var (
	verbose           *bool
	console           *bool
	debugSelectorsStr *string
	startTime time.Time
)

type Logging struct {
	Selectors  []string

	Files     *bool
	Rotate    *Rotate
	Syslog    *bool

	Level      string
	Json       bool
}

func init() {
	startTime = time.Now()

	verbose = flag.Bool("v", false, "Log at INFO level")
	console = flag.Bool("e", false, "Log to stderr and disable syslog/file output")
	debugSelectorsStr = flag.String("d", "", "Enable certain debug selectors")
}

func HandleFlags(name string) error {
	level := _log.level
	if *verbose {
		if LOG_INFO > level {
			level = LOG_INFO
		}
	}

	selectors := strings.Split(*debugSelectorsStr, ",")
	debugSelectors, debugAll := parseSelectors(selectors)
	if debugAll || len(debugSelectors) > 0 {
		level = LOG_DEBUG
	}

	_log.level = level
	_log.toConsole = true
	_log.logger = log.New(os.Stderr, name, stderrLogFlags)
	_log.selectors = debugSelectors
	_log.debugAllSelectors = debugAll

	return nil
}

func Init(name string, config *Logging) error {
	_log = logger{
		JSON: config.Json,
	}

	logLevel, err := getLogLevel(config)
	if err != nil {
		return err
	}

	if *verbose {
		if LOG_INFO > logLevel {
			logLevel = LOG_INFO
		}
	}

	debugSelectors := config.Selectors
	if logLevel == LOG_DEBUG {
		if len(debugSelectors) == 0 {
			debugSelectors = []string{"*"}
		}
	}
	if len(*debugSelectorsStr) > 0 {
		debugSelectors = strings.Split(*debugSelectorsStr, ",")
		logLevel = LOG_DEBUG
	}

	defaultFilePath := paths.Resolve(paths.Logs, "")

	var Syslog, Files bool
	if config.Syslog != nil {
		Syslog = *config.Syslog
	} else {
		Syslog = false
	}
	if config.Files != nil {
		Files = *config.Files
	} else {
		Files = true
	}

	if *console {
		Syslog = false
		Files = false
	}

	LogInit(Priority(logLevel), "", Syslog, true, debugSelectors)
	if len(debugSelectors) > 0 {
		config.Selectors = debugSelectors
	}

	if Files {
		if config.Rotate == nil {
			config.Rotate = &Rotate{
				Path: defaultFilePath,
				Name: name,
			}
		} else {
			if config.Rotate.Path == "" {
				config.Rotate.Path = defaultFilePath
			}

			if config.Rotate.Name == "" {
				config.Rotate.Name = name
			}
		}

		err := SetToFile(true, config.Rotate)
		if err != nil {
			return err
		}
	}

	if IsDebug("stdlog") {
		log.SetOutput(ioutil.Discard)
	}

	SetConsole()
	return nil
}

func SetConsole() {
	if !*console {
		SetToConsole(false, "")
		Debug("log", "Disable stderr logging")
	}
}

func getLogLevel(config *Logging) (Priority, error) {
	if config == nil || config.Level == "" {
		return LOG_INFO, nil
	}

	levels := map[string]Priority{
		"critical": LOG_CRIT,
		"error":    LOG_ERR,
		"warning":  LOG_WARNING,
		"info":     LOG_INFO,
		"debug":    LOG_DEBUG,
	}

	level, ok := levels[strings.ToLower(config.Level)]
	if !ok {
		return 0, fmt.Errorf("unknown log level: %v", config.Level)
	}
	return level, nil
}