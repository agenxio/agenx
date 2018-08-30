package console

import (
	"os"
	"fmt"
	"bufio"
	"runtime"

	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/outputs"
	"github.com/queueio/sentry/utils/component"
)

var consoleDebug = log.MakeDebug("console")

type console struct {
	name    string
	stdout *os.File
	writer *bufio.Writer
	buffer  int
	end     byte
}

func init() {
	outputs.Register("console", open)
}

func open(info component.Info, config *config.Config) (queue.Handler, error) {
	/*
	cfg := defaultConfig
	err := config.Unpack(&config)
	if err != nil {
		return outputs.Fail(err)
	}
	*/

	c := &console{
		name: info.Component,
        stdout: os.Stdout,
        buffer: 8*1024,
        end: '\n',
    }

	// check stdout actually being available
	if runtime.GOOS != "windows" {
		if _, err := c.stdout.Stat(); err != nil {
			err = fmt.Errorf("console output initialization failed with: %v", err)
			return outputs.Fail(err)
		}
	}

	c.writer = bufio.NewWriterSize(c.stdout, c.buffer)
	return c, nil
}

func (c *console) Close() error { return nil }

func (c *console) HandleMessage(message *queue.Message) error {
	fmt.Println(string(message.Body))
	/*
    err := client(c.writer, message.Body, c.end)
    if err != nil {
		return err
	}
	*/

	return nil
}

func (c *console) LogFailedMessage(message *queue.Message) {
}