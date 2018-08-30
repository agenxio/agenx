package reader

import (
	"errors"
	"time"
)

var (
	errTimeout = errors.New("timeout")
)

type Timeout struct {
	reader  Reader
	timeout time.Duration
	signal  error
	running bool
	ch      chan lineMessage
}

type lineMessage struct {
	line Message
	err  error
}

func NewTimeout(reader Reader, signal error, t time.Duration) *Timeout {
	if signal == nil {
		signal = errTimeout
	}

	return &Timeout{
		reader:  reader,
		signal:  signal,
		timeout: t,
		ch:      make(chan lineMessage, 1),
	}
}

func (p *Timeout) Next() (Message, error) {
	if !p.running {
		p.running = true
		go func() {
			for {
				message, err := p.reader.Next()
				p.ch <- lineMessage{message, err}
				if err != nil {
					break
				}
			}
		}()
	}

	select {
	case msg := <-p.ch:
		if msg.err != nil {
			p.running = false
		}
		return msg.line, msg.err
	case <-time.After(p.timeout):
		return Message{}, p.signal
	}
}