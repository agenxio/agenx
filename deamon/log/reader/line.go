package reader

import (
	"bufio"
	"io"
)

type Line struct {
	*bufio.Scanner
}

func NewLine(input io.Reader) (*Line, error) {
	line := &Line{bufio.NewScanner(input)}
	line.Split(bufio.ScanLines)
	return line, nil
}

func (l *Line) Next() ([]byte, int, error) {
	if l.Scan() && l.Err() == nil {
		return l.Bytes(), len(l.Bytes()), nil
	}

	return nil, 0, l.Err()
}