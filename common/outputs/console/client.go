package console

import (
	"bufio"
)

func client(writer *bufio.Writer, data []byte, end byte) error {
	_, err := writer.Write(data)
	if err != nil {
		return err
	}

	if err := writer.WriteByte(end); err != nil {
		return err
	}

	return nil
}
