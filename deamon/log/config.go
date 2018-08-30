package log

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/beats/libbeat/common/match"

	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/components/scribe/log/reader"
	"github.com/queueio/sentry/components/scribe"
)

type PublisherConfig struct {
	Type string `config:"type"`
}

var (
	defaultConfig = config{
		PublisherConfig: PublisherConfig{
			Type: "log",
		},
		Name: "test",
		Enabled:        true,
		Visitor: VisitorConfig{
			10 * time.Second,
		},
		Ignore: Ignore{0},
		Symlinks:       false,
		Tail: Tail{
			false,
		},
		Max: Max{
			10 * humanize.MiByte,
		},
		LogConfig: LogConfig{
			BackOff: backOffConfig{
				Min: 1 * time.Second,
				Max: 10 * time.Second,
				Factor: 2,
			},
			Scanner: scannerConfig{
				Limit: 0,
				Order:ScanOrderAsc,
				Sort:ScanSortNone,
				Buffer: 16 * humanize.KiByte,
				Close: closeConfig{
					Removed: true,
					Renamed: false,
					EOF: false,
					Timeout: 0,
				},
			},
		},
	}
)

type config struct {
	PublisherConfig    `config:",inline"`
	LogConfig            `config:",inline"`

	Name         string
	State        StateConfig

	Enabled      bool            `config:"enabled"`
	Ignore       Ignore
	Paths        []string        `config:"paths"`
	Visitor      VisitorConfig


	Symlinks     bool            `config:"symlinks"`
	Tail 	     Tail
	Recursive    Recursive

	Scanner      scannerConfig
	Encoding     string `config:"encoding"`

	Exclude	     Exclude
	Include      Include

	Max          Max
	Multiline    *reader.MultilineConfig `config:"multiline"`
	JSON         *reader.JSONConfig      `config:"json"`
}

type Max struct {
	Bytes  int
}

type Ignore struct {
	Older time.Duration
}

type Recursive struct {
	Enabled bool
}

type VisitorConfig struct {
	Frequency  time.Duration  `config:"frequency" validate:"min=0,nonzero"`
}

type ScannerConfig struct {
	Limit      uint64         `config:"limit" validate:"min=0"`
	Order      string         `config:"order"`
	Sort       string         `config:"sort"`
}

type StateConfig struct {
	Clean
}

type Clean struct {
	Inactive time.Duration  `config:"inactive" validate:"min=0"`
	Removed   bool          `config:"removed"`
}

type Include struct {
	Lines []match.Matcher
}

type Exclude struct {
	Lines []match.Matcher
	Files   []match.Matcher
}

type Tail struct {
	Files   bool
}

type LogConfig struct {
	BackOff  backOffConfig
	Scanner  scannerConfig
}

type backOffConfig struct {
    Min     time.Duration  `config:"min" validate:"min=0,nonzero"`
    Max     time.Duration  `config:"max" validate:"min=0,nonzero"`
    Factor  int            `config:"factor" validate:"min=1"`
}

type scannerConfig struct {
	Limit   uint64  `config:"limit" validate:"min=0"`
	Order   string  `config:"order"`
	Sort    string  `config:"sort"`
	Buffer  int     `config:"buffer"`
	Close   closeConfig
}

type closeConfig struct {
	Inactive time.Duration `config:"inactive"`
	Removed  bool          `config:"removed"`
	Renamed  bool          `config:"renamed"`
	EOF      bool          `config:"eof"`
	Timeout  time.Duration `config:"timeout" validate:"min=0"`
}

const (
	ScanOrderAsc     = "asc"
	ScanOrderDesc    = "desc"
	ScanSortNone     = ""
	ScanSortModtime  = "modtime"
	ScanSortFilename = "filename"
)

var ValidScanOrder = map[string]struct{}{
	ScanOrderAsc:  {},
	ScanOrderDesc: {},
}

var ValidScanSort = map[string]struct{}{
	ScanSortNone:     {},
	ScanSortModtime:  {},
	ScanSortFilename: {},
}

func (c *config) Validate() error {
	if c.Type == scribe.LogType && len(c.Paths) == 0 {
		return fmt.Errorf("No paths were defined for prospector")
	}

	if c.State.Clean.Inactive != 0 && c.Ignore.Older == 0 {
		return fmt.Errorf("ignore_older must be enabled when clean_inactive is used")
	}

	if c.State.Clean.Inactive != 0 && c.State.Clean.Inactive <= c.Ignore.Older+c.Visitor.Frequency {
		return fmt.Errorf("clean_inactive must be > ignore_older + scan_frequency to make sure only files which are not monitored anymore are removed")
	}

	if _, ok := scribe.ValidType[c.Type]; !ok {
		return fmt.Errorf("Invalid input type: %v", c.Type)
	}

	if c.JSON != nil && len(c.JSON.MessageKey) == 0 &&
		c.Multiline != nil {
		return fmt.Errorf("When using the JSON decoder and multiline together, you need to specify a message_key value")
	}

	if c.JSON != nil && len(c.JSON.MessageKey) == 0 &&
		(len(c.Include.Lines) > 0 || len(c.Exclude.Lines) > 0) {
		return fmt.Errorf("When using the JSON decoder and line filtering together, you need to specify a message_key value")
	}

	if c.Scanner.Sort != "" {
		log.Warn("scan_sort is used.")

		if _, ok := ValidScanSort[c.Scanner.Sort]; !ok {
			return fmt.Errorf("Invalid scan sort: %v", c.Scanner.Sort)
		}

		if _, ok := ValidScanOrder[c.Scanner.Sort]; !ok {
			return fmt.Errorf("Invalid scan order: %v", c.Scanner.Sort)
		}
	}

	return nil
}

func (c *config) resolvePaths() error {
	var paths []string
	if !c.Recursive.Enabled {
		log.Debug("prospector", "recursive glob disabled")
		paths = c.Paths
	} else {
		log.Debug("prospector", "recursive glob enabled")
	}
	for _, path := range c.Paths {
		patterns, err := scribe.GlobPatterns(path, recursiveGlobDepth)
		if err != nil {
			return err
		}
		if len(patterns) > 1 {
			log.Debug("prospector", "%q expanded to %#v", path, patterns)
		}
		paths = append(paths, patterns...)
	}
	c.Paths = paths
	return nil
}