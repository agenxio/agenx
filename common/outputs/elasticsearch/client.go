package elasticsearch

import (
	"time"
	"encoding/json"

	"github.com/satori/go.uuid"
	"github.com/olivere/elastic"

	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/outputs"
	"github.com/queueio/sentry/utils/types/event"
)

type Client struct {
	id        uuid.UUID
	channel  *chan *queue.Message
	producer *elastic.Client
	bulk     *bulk
}

func (c *Client) ID() uuid.UUID {
	return c.id
}

func (c *Client) Run() error {
    for {
        select {
        case msg, open := <-*c.channel:
        	if !open {
        		c.Stop()
                return outputs.ErrClosed
            }

            var event event.Event
            if err := json.Unmarshal(msg.Body, &event); err != nil {
            	log.Err("json unmarshal error: %s", err.Error())
            	continue
			}

			key := event.Topic
			if bulk, ok := c.bulk.service[key]; ok {
				bulk.service.Add(elastic.NewBulkIndexRequest().Id(string(msg.ID[:])).Doc(event.Fields))
				if bulk.service.NumberOfActions() >= c.bulk.size {
					response, err := bulk.service.Do()
					if err != nil {
						return err
					}
					if response.Errors {
						return ErrBulkCommit
					}

					log.Debug("client", "elasticSearch bulk success")
				}

				bulk.timestamp = time.Now().Unix()
			} else {
				c.bulk.service[key] = &service{
					timestamp: time.Now().Unix(),
					service: c.producer.Bulk().Index(key).Type(docType),
				}
				log.Debug("client", "elasticSearch bulk index create")
			}

		default:
			if err := c.bulk.flush(); err != nil {
				log.Err("flush error: %s", err.Error())
			}
        }
    }

	return nil
}

func (c *Client) Stop() {
	c.producer.Stop()
}