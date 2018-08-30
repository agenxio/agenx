package log

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"golang.org/x/text/transform"

	"github.com/queueio/sentry/utils/log"
cfg	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/outputs"
	"github.com/queueio/sentry/utils/types/maps"
	"github.com/queueio/sentry/utils/types/event"

	"github.com/queueio/sentry/components/scribe"
	"github.com/queueio/sentry/components/scribe/log/reader"

)

var (
	ErrFileTruncate = errors.New("detected file being truncated")
	ErrRenamed      = errors.New("file was renamed")
	ErrRemoved      = errors.New("file was removed")
	ErrInactive     = errors.New("file inactive")
	ErrClosed       = errors.New("reader closed")
)

type Scanner struct {
	id        uuid.UUID
	config    config
	source    scribe.Source // the source being watched

	done      chan struct{}
	stopOnce  sync.Once
	stopWg   *sync.WaitGroup

	state     scribe.State
	states   *scribe.States
	log      *Log

	reader    reader.Reader
	publisher outputs.Publisher
}

func NewScanner(config *cfg.Config, state scribe.State, states *scribe.States, pub outputs.Publisher) (*Scanner, error) {
	s := &Scanner{
		config:     defaultConfig,
		state:      state,
		states:     states,
		publisher:  pub,
		done:       make(chan struct{}),
		stopWg:     &sync.WaitGroup{},
		id:         uuid.NewV4(),
	}

	if err := config.Unpack(&s.config); err != nil {
		return nil, err
	}

	if s.config.State.Clean.Inactive > 0 {
		s.state.TTL = s.config.State.Clean.Inactive
	}
	return s, nil
}

func (s *Scanner) open() error {
	switch s.config.Type {
	case scribe.LogType:
		return s.openFile()
	default:
		return fmt.Errorf("Invalid scanner type: %+v", s.config)
	}
}

func (s *Scanner) ID() uuid.UUID {
	return s.id
}

func (s *Scanner) Setup() error {
	err := s.open()
	if err != nil {
		return fmt.Errorf("Scanner setup failed. Unexpected file opening error: %s", err)
	}

	s.reader, err = s.newLogFileReader()
	if err != nil {
		if s.source != nil {
			s.source.Close()
		}
		return fmt.Errorf("Scanner setup failed. Unexpected encoding line reader error: %s", err)
	}

	return nil
}

func (w *Scanner) Run() error {
	w.stopWg.Add(1)
	select {
	case <-w.done:
		w.stopWg.Done()
		return nil
	default:
	}

	defer func() {
		w.stop()
		w.cleanup()
		w.stopWg.Done()
	}()

	go func() {

		closeTimeout := make(<-chan time.Time)
		if w.config.Scanner.Close.Timeout > 0 {
			closeTimeout = time.After(w.config.Scanner.Close.Timeout)
		}

		select {
		case <-closeTimeout:
			log.Info("Closing scanner because close_timeout was reached.")
		case <-w.done:
		}

		w.stop()
		w.log.Close()
	}()

	log.Info("Scanner started for file: %s", w.state.Source)

	for {
		select {
		case <-w.done:
			return nil
		default:
		}

		message, err := w.reader.Next()
		if err != nil {
			switch err {
			case ErrFileTruncate:
				log.Info("File was truncated. Begin reading file from offset 0: %s", w.state.Source)
				w.state.Offset = 0
			case ErrRemoved:
				log.Info("File was removed: %s. Closing because close_removed is enabled.", w.state.Source)
			case ErrRenamed:
				log.Info("File was renamed: %s. Closing because close_renamed is enabled.", w.state.Source)
			case ErrClosed:
				log.Info("Reader was closed: %s. Closing.", w.state.Source)
			case io.EOF:
				log.Info("End of file reached: %s. Closing because close_eof is enabled.", w.state.Source)
			case ErrInactive:
				log.Info("File is inactive: %s. Closing because close_inactive of %v reached.", w.state.Source, w.config.Scanner.Close.Inactive)
			default:
				log.Err("Read line error: %s; File: ", err, w.state.Source)
			}
			return nil
		}

		if w.state.Offset == 0 {
			message.Content = bytes.Trim(message.Content, "\xef\xbb\xbf")
		}

		state := w.getState()
		state.Offset += int64(message.Bytes)

		data := scribe.NewData()
		if w.source.HasState() {
			data.SetState(state)
		}

		text := string(message.Content)
		if !message.IsEmpty() && w.shouldExportLine(text) {
			fields := maps.StringIf{
				"source": state.Source,
				"offset": state.Offset, // Offset here is the offset before the starting char.
			}
			fields.DeepUpdate(message.Fields)

			/*
			var jsonFields maps.StringIf
			if f, ok := fields["json"]; ok {
				jsonFields = f.(maps.StringIf)
			}

			if w.config.JSON != nil && len(jsonFields) > 0 {
				reader.MergeJSONFields(fields, jsonFields, &text, *w.config.JSON)
			} else*/ if &text != nil {
				if fields == nil {
					fields = maps.StringIf{}
				}
				fields["message"] = text
			}

			data.Event = event.Event{
				Topic:     w.config.Name,
				Timestamp: message.Ts,
				Fields:    fields,
			}
		}

		if !w.sendEvent(data) {
			return nil
		}

		w.state = state
	}
}

func (s *Scanner) stop() {
	s.stopOnce.Do(func() {
		close(s.done)
	})
}

func (s *Scanner) Stop() {
	s.stop()
	s.stopWg.Wait()
}

func (s *Scanner) sendEvent(data *scribe.Data) bool {
	if s.source.HasState() {
		s.states.Update(data.GetState())
	}

	err := s.publisher.Publish(data.Event)
	return err == nil
}

func (s *Scanner) SendStateUpdate() {
	if !s.source.HasState() {
		return
	}

	log.Debug("scanner", "Update state: %s, offset: %v", s.state.Source, s.state.Offset)
	s.states.Update(s.state)

	d := scribe.NewData()
	d.SetState(s.state)
}

func (s *Scanner) shouldExportLine(line string) bool {
	if len(s.config.Include.Lines) > 0 {
		if !scribe.MatchAny(s.config.Include.Lines, line) {
			log.Debug("scanner", "Drop line as it does not match any of the include patterns %s", line)
			return false
		}
	}
	if len(s.config.Exclude.Lines) > 0 {
		if scribe.MatchAny(s.config.Exclude.Lines, line) {
			log.Debug("scanner", "Drop line as it does match one of the exclude patterns%s", line)
			return false
		}
	}

	return true
}

func (s *Scanner) openFile() error {
	f, err := scribe.ReadOpen(s.state.Source)
	if err != nil {
		return fmt.Errorf("Failed opening %s: %s", s.state.Source, err)
	}

	err = s.validateFile(f)
	if err != nil {
		f.Close()
		return err
	}

	s.source = File{File: f}
	return nil
}

func (s *Scanner) validateFile(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("Failed getting stats for file %s: %s", s.state.Source, err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("Tried to open non regular file: %q %s", info.Mode(), info.Name())
	}

	if !os.SameFile(s.state.Fileinfo, info) {
		return errors.New("file info is not identical with opened file. Aborting harvesting and retrying file later again")
	}

	if err != nil {
		if err == transform.ErrShortSrc {
			log.Info("Initialising encoding for '%v' failed due to file being too short", f)
		} else {
			log.Err("Initialising encoding for '%v' failed: %v", f, err)
		}
		return err
	}

	offset, err := s.initFileOffset(f)
	if err != nil {
		return err
	}

	log.Debug("scanner", "Setting offset for file: %s. Offset: %d ", s.state.Source, offset)
	s.state.Offset = offset

	return nil
}

func (s *Scanner) initFileOffset(file *os.File) (int64, error) {
	if s.state.Offset > 0 {
		log.Debug("scanner", "Set previous offset for file: %s. Offset: %d ", s.state.Source, s.state.Offset)
		return file.Seek(s.state.Offset, os.SEEK_SET)
	}

	log.Debug("scanner", "Setting offset for file based on seek: %s", s.state.Source)
	return file.Seek(0, os.SEEK_CUR)
}

func (s *Scanner) getState() scribe.State {
	if !s.source.HasState() {
		return scribe.State{}
	}
	this := s.state
	this.FileStateOS = scribe.GetOSState(s.state.Fileinfo)
	return this
}

func (s *Scanner) cleanup() {
	s.state.Finished = true

	log.Debug("scanner", "Stopping scanner for file: %s", s.state.Source)
	defer log.Debug("scanner", "scanner cleanup finished for file: %s", s.state.Source)

	if s.source != nil {
		s.source.Close()

		log.Debug("scanner", "Closing file: %s", s.state.Source)
		s.SendStateUpdate()
	} else {
		log.Warn("Stopping scanner, NOT closing file as file info not available: %s", s.state.Source)
	}
}

func (s *Scanner) newLogFileReader() (reader.Reader, error) {
	var r reader.Reader
	var err error

	s.log, err = NewLog(s.source, s.config.LogConfig)
	if err != nil {
		return nil, err
	}

	r, err = reader.NewEncode(s.log)
	if err != nil {
		return nil, err
	}

	if s.config.JSON != nil {
		r = reader.NewJSON(r, s.config.JSON)
	}

	r = reader.NewStripNewline(r)

	if s.config.Multiline != nil {
		r, err = reader.NewMultiLine(r, "\n", s.config.Max.Bytes, s.config.Multiline)
		if err != nil {
			return nil, err
		}
	}

	return reader.NewLimit(r, s.config.Max.Bytes), nil
}