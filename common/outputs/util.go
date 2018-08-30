package outputs

import (
	"github.com/queueio/sentry/utils/queue"
	"errors"
)

var (
    ErrEmpty  = errors.New("channel was empty")
    ErrFull   = errors.New("channel was full")
    ErrClosed = errors.New("channel closed")
)

func Fail(err error) (queue.Handler, error) { return nil, err }
