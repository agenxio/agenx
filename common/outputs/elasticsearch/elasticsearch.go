package elasticsearch

import (
	"github.com/satori/go.uuid"
	"github.com/olivere/elastic"

	"github.com/queueio/sentry/utils/job"
	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/outputs"
	"github.com/queueio/sentry/utils/component"
)

const (
	docType = "doc"
)

type elasticSearch struct {
	executor *job.Executor
	channel  *chan *queue.Message
}

func init() {
	outputs.Register("elasticSearch", open)
}

func open(info component.Info, config *config.Config) (queue.Handler, error) {
	cfg := defaultConfig
	err := config.Unpack(&cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	c := make(chan *queue.Message, cfg.Size.Channel)
	es := &elasticSearch{
		executor: job.New(),
		channel:  &c,
	}

	for _, host := range cfg.Hosts {
		for i := 0; i < cfg.Size.Worker; i++ {
			client, err := elastic.NewClient(elastic.SetURL(host), elastic.SetSniff(false),
							elastic.SetMaxRetries(cfg.Size.Retries), elastic.SetHealthcheck(false))
			if err != nil {
				log.Err("host[%s] error: %s", host, err.Error())
				continue
			}

			log.Debug("elasticSearch", "client create is ok")

			es.executor.Start(&Client{
				id:       uuid.NewV4(),
				channel:  &c,
				producer: client,
				bulk:     &bulk{
					size:    cfg.Bulk.Size,
					timeout: int64(cfg.Bulk.Timeout),
					service: map[string]*service{},
				},
			})
		}
	}

	return es, nil
}

func (e *elasticSearch) Close() error { return nil }

func (e *elasticSearch) HandleMessage(message *queue.Message) error {
	*e.channel <- message
	log.Debug("elasticSearch", "message to channel ok")
	return nil
}

func (e *elasticSearch) LogFailedMessage(message *queue.Message) {
}