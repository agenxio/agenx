package node

import (
	"time"
	"encoding/json"

	"github.com/satori/go.uuid"

	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/outputs"
	"github.com/queueio/sentry/utils/types/event"
)

type Client struct {
	id         uuid.UUID

    timer     *time.Ticker
    channel   *chan event.Event

	producer  *queue.Producer
}

func (c *Client) ID() uuid.UUID {
	return c.id
}

func (c *Client) Run() error {
    for {
        select {
        case event, open := <-*c.channel:
        	if !open {
        		c.Stop()
                return outputs.ErrClosed
            }

            body, err := json.Marshal(event)
            if err != nil {
            	log.Err(err.Error())
            	continue
			}

            err = c.producer.Publish(event.Topic, body)
            if err != nil {
            	log.Err(err.Error())
			}
        case <-c.timer.C:
        	if err := c.producer.Ping(); err != nil {
        		log.Warn(err.Error())
			}
        	log.Warn(outputs.ErrEmpty.Error())
        }
    }

	return nil
}

func (c *Client) Stop() {
	c.producer.Stop()
}