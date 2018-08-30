package node

import (
	"time"
	"strconv"
	"strings"

	"github.com/satori/go.uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/queueio/sentry/utils/job"
	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/types"
	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/httpx"
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/outputs"
	"github.com/queueio/sentry/utils/encoding"
	"github.com/queueio/sentry/utils/component"
	"github.com/queueio/sentry/utils/types/event"
)

type node struct {
	executor *job.Executor
	channels  []*chan event.Event
	n         int

	RemoteAddress  string  `mapstructure:"remote_address"`
	TCPPort        float64 `mapstructure:"tcp_port"`
}

func init() {
	outputs.Register("node", open)
}

func open(info component.Info, config *config.Config) (queue.Handler, error) {
	cfg := defaultConfig
	err := config.Unpack(&cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	n := &node{
        executor: job.New(),
    }

    var v interface{}
    client := httpx.NewClient(nil, cfg.Timeout.Connect, cfg.Timeout.Request)
    if err := client.GETV1(cfg.URLs[0], &v); err != nil {
    	return nil, err
	}

	nodes := v.(map[string]interface{})["producers"].([]interface{})
	if l := len(nodes); l > 0 {
		if err := mapstructure.Decode(nodes[encoding.Hashcode(info.Hostname) % l], &n); err != nil {
			return nil, err
		}
	}

	port := strconv.FormatFloat(n.TCPPort, 'f', -1, 64)
	for i := 0; i < cfg.Publish.Group; i++ {
		producer, err := queue.NewProducer(n.RemoteAddress[:strings.Index(n.RemoteAddress, ":")+1]+port,
											queue.NewConfig())
		if err != nil {
			log.Err(err.Error())
			continue
		}

		channel := make(chan event.Event, cfg.Publish.Size)
		n.channels = append(n.channels, &channel)
		n.executor.Start(&Client{
			id:       uuid.NewV4(),
			channel:  &channel,
			timer:    time.NewTicker(cfg.Publish.Timer),
			producer: producer,
		})
	}

	n.n = len(n.channels)
	return n, nil
}

func (n *node) Group(name string) types.Object {
	index := encoding.Hashcode(name) % n.n
	return &publisher{
		index: index,
		channel: n.channels[index],
	}
}

func (n *node) Close() error { return nil }

func (n *node) HandleMessage(message *queue.Message) error {
	return nil
}

func (n *node) LogFailedMessage(message *queue.Message) {
}
