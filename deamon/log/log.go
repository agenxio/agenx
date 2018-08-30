package log

import (
	"io"
	"os"
	"time"
	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/components/scribe"
)

type Log struct {
	fs           scribe.Source
	offset       int64
	config       LogConfig
	lastTimeRead time.Time
	backoff      time.Duration
	done         chan struct{}
}

func NewLog(fs scribe.Source, config LogConfig) (*Log, error) {
	var offset int64
	if seeker, ok := fs.(io.Seeker); ok {
		var err error
		offset, err = seeker.Seek(0, os.SEEK_CUR)
		if err != nil {
			return nil, err
		}
	}

	return &Log{
		fs:           fs,
		offset:       offset,
		config:       config,
		lastTimeRead: time.Now(),
		backoff:      config.BackOff.Max,
		done:         make(chan struct{}),
	}, nil
}

func (f *Log) Read(buf []byte) (int, error) {
	totalN := 0

	for {
		select {
		case <-f.done:
			return 0, ErrClosed
		default:
		}

		n, err := f.fs.Read(buf)
		if n > 0 {
			f.offset += int64(n)
			f.lastTimeRead = time.Now()
		}
		totalN += n

		if err == nil {
			f.backoff = f.config.BackOff.Min
			return totalN, nil
		}

		buf = buf[n:]

		err = f.errorChecks(err)
		if err != nil || len(buf) == 0 {
			return totalN, err
		}

		log.Debug("worker", "End of file reached: %s; Backoff now.", f.fs.Name())
		f.wait()
	}
}

func (f *Log) errorChecks(err error) error {
	if err != io.EOF {
		log.Err("Unexpected state reading from %s; error: %s", f.fs.Name(), err)
		return err
	}

	if !f.fs.Continuable() {
		log.Debug("worker", "Source is not continuable: %s", f.fs.Name())
		return err
	}

	if err == io.EOF && f.config.Scanner.Close.EOF {
		return err
	}

	info, statErr := f.fs.Stat()
	if statErr != nil {
		log.Err("Unexpected error reading from %s; error: %s", f.fs.Name(), statErr)
		return statErr
	}

	if info.Size() < f.offset {
		log.Debug("worker",
			"File was truncated as offset (%d) > size (%d): %s", f.offset, info.Size(), f.fs.Name())
		return ErrFileTruncate
	}

	age := time.Since(f.lastTimeRead)
	if age > f.config.Scanner.Close.Inactive {
		return ErrInactive
	}

	if f.config.Scanner.Close.Renamed {
		if !scribe.IsSameFile(f.fs.Name(), info) {
			return ErrRenamed
		}
	}

	if f.config.Scanner.Close.Removed {
		_, statErr := os.Stat(f.fs.Name())
		if statErr != nil {
			return ErrRemoved
		}
	}

	return nil
}

func (f *Log) wait() {
	select {
	case <-f.done:
		return
	case <-time.After(f.backoff):
	}

	if f.backoff < f.config.BackOff.Max {
		f.backoff = f.backoff * time.Duration(f.config.BackOff.Factor)
		if f.backoff > f.config.BackOff.Max {
			f.backoff = f.config.BackOff.Max
		}
	}
}

func (f *Log) Close() {
	close(f.done)
}