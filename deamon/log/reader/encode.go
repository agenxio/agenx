package reader

import (
	"io"
	"time"
)

type Encode struct {
	reader *Line
}

func NewEncode(reader io.Reader) (Encode, error) {
	r, err := NewLine(reader)
	return Encode{r}, err
}

func (e Encode) Next() (Message, error) {
	c, sz, err := e.reader.Next()
	return Message{
		Ts:      time.Now(),
		Content: c,
		Bytes:   sz,
	}, err
}