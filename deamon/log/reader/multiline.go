package reader

import (
	"errors"
	"fmt"
	"time"

	"github.com/queueio/sentry/utils/log"
	"github.com/elastic/beats/libbeat/common/match"
)

type Multiline struct {
	reader       Reader
	pred         matcher
	flushMatcher *match.Matcher
	maxBytes     int // bytes stored in content
	maxLines     int
	separator    []byte
	last         []byte
	numLines     int
	err          error // last seen error
	state        func(*Multiline) (Message, error)
	message      Message
}

const (
	defaultMaxLines = 500
	defaultMultilineTimeout = 5 * time.Second
)

type matcher func(last, current []byte) bool

var (
	sigMultilineTimeout = errors.New("multline timeout")
)

func NewMultiLine(reader Reader, separator string, maxBytes int, config *MultilineConfig) (*Multiline, error) {
	types := map[string]func(match.Matcher) (matcher, error){
		"before": beforeMatcher,
		"after":  afterMatcher,
	}

	matcherType, ok := types[config.Match]
	if !ok {
		return nil, fmt.Errorf("unknown matcher type: %s", config.Match)
	}

	matcher, err := matcherType(*config.Pattern)
	if err != nil {
		return nil, err
	}

	flushMatcher := config.FlushPattern

	if config.Negate {
		matcher = negatedMatcher(matcher)
	}

	maxLines := defaultMaxLines
	if config.MaxLines != nil {
		maxLines = *config.MaxLines
	}

	timeout := defaultMultilineTimeout
	if config.Timeout != nil {
		timeout = *config.Timeout
		if timeout < 0 {
			return nil, fmt.Errorf("timeout %v must not be negative", config.Timeout)
		}
	}

	if timeout > 0 {
		reader = NewTimeout(reader, sigMultilineTimeout, timeout)
	}

	mlr := &Multiline{
		reader:       reader,
		pred:         matcher,
		flushMatcher: flushMatcher,
		state:        (*Multiline).readFirst,
		maxBytes:     maxBytes,
		maxLines:     maxLines,
		separator:    []byte(separator),
		message:      Message{},
	}
	return mlr, nil
}

func (mlr *Multiline) Next() (Message, error) {
	return mlr.state(mlr)
}

func (mlr *Multiline) readFirst() (Message, error) {
	for {
		message, err := mlr.reader.Next()
		if err != nil {
			if err == sigMultilineTimeout {
				continue
			}

			log.Debug("multiline", "Multiline event flushed because timeout reached.")
			return message, err
		}

		if message.Bytes == 0 {
			continue
		}

		mlr.clear()
		mlr.load(message)
		mlr.setState((*Multiline).readNext)
		return mlr.readNext()
	}
}

func (mlr *Multiline) readNext() (Message, error) {
	for {
		message, err := mlr.reader.Next()
		if err != nil {
			if err == sigMultilineTimeout {
				if mlr.numLines == 0 {
					continue
				}

				log.Debug("multiline", "Multiline event flushed because timeout reached.")

				msg := mlr.finalize()
				mlr.resetState()
				return msg, nil
			}

			if message.Bytes == 0 {
				if mlr.numLines == 0 {
					return Message{}, err
				}

				msg := mlr.finalize()
				mlr.err = err
				mlr.setState((*Multiline).readFailed)
				return msg, nil
			}

			if mlr.message.Bytes == 0 || mlr.pred(mlr.last, message.Content) {
				mlr.addLine(message)
				msg := mlr.finalize()
				mlr.err = err
				mlr.setState((*Multiline).readFailed)
				return msg, nil
			}

			msg := mlr.finalize()
			mlr.load(message)
			return msg, nil
		}

		if mlr.flushMatcher != nil {
			endPatternReached := (mlr.flushMatcher.Match(message.Content))

			if endPatternReached == true {
				mlr.addLine(message)
				msg := mlr.finalize()
				mlr.resetState()
				return msg, nil
			}
		}

		if mlr.message.Bytes > 0 && !mlr.pred(mlr.last, message.Content) {
			msg := mlr.finalize()
			mlr.load(message)
			return msg, nil
		}

		mlr.addLine(message)
	}
}

func (mlr *Multiline) readFailed() (Message, error) {
	err := mlr.err
	mlr.err = nil
	mlr.resetState()
	return Message{}, err
}

func (mlr *Multiline) load(m Message) {
	mlr.addLine(m)
	mlr.message.Ts = m.Ts
	mlr.message.AddFields(m.Fields)
}

func (mlr *Multiline) clear() {
	mlr.message = Message{}
	mlr.last = nil
	mlr.numLines = 0
	mlr.err = nil
}

func (mlr *Multiline) finalize() Message {
	msg := mlr.message
	mlr.clear()
	return msg
}

func (mlr *Multiline) addLine(m Message) {
	if m.Bytes <= 0 {
		return
	}

	sz := len(mlr.message.Content)
	addSeparator := len(mlr.message.Content) > 0 && len(mlr.separator) > 0
	if addSeparator {
		sz += len(mlr.separator)
	}

	space := mlr.maxBytes - sz

	maxBytesReached := (mlr.maxBytes <= 0 || space > 0)
	maxLinesReached := (mlr.maxLines <= 0 || mlr.numLines < mlr.maxLines)

	if maxBytesReached && maxLinesReached {
		if space < 0 || space > len(m.Content) {
			space = len(m.Content)
		}

		tmp := mlr.message.Content
		if addSeparator {
			tmp = append(tmp, mlr.separator...)
		}
		mlr.message.Content = append(tmp, m.Content[:space]...)
		mlr.numLines++
	}

	mlr.last = m.Content
	mlr.message.Bytes += m.Bytes
	mlr.message.AddFields(m.Fields)
}

func (mlr *Multiline) resetState() {
	mlr.setState((*Multiline).readFirst)
}

func (mlr *Multiline) setState(next func(mlr *Multiline) (Message, error)) {
	mlr.state = next
}

func afterMatcher(pat match.Matcher) (matcher, error) {
	return genPatternMatcher(pat, func(last, current []byte) []byte {
		return current
	})
}

func beforeMatcher(pat match.Matcher) (matcher, error) {
	return genPatternMatcher(pat, func(last, current []byte) []byte {
		return last
	})
}

func negatedMatcher(m matcher) matcher {
	return func(last, current []byte) bool {
		return !m(last, current)
	}
}

func genPatternMatcher(
	pat match.Matcher,
	sel func(last, current []byte) []byte,
) (matcher, error) {
	matcher := func(last, current []byte) bool {
		line := sel(last, current)
		return pat.Match(line)
	}
	return matcher, nil
}