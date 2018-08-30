package elasticsearch

import (
	"time"
	"errors"

	"github.com/olivere/elastic"
	"github.com/queueio/sentry/utils/log"
	"fmt"
)

var ErrBulkCommit = errors.New("bulk commit failed")

type bulk struct {
	size     int
	timeout  int64
    service  map[string]*service
}

type service struct {
	timestamp  int64  // record last time
    service   *elastic.BulkService
}

func (b *bulk) flush() error {
	var error error = nil

	current := time.Now().Unix()
	for k, v := range b.service {
		if time := current - v.timestamp; time > b.timeout {
			if v.service.NumberOfActions() > 0 {
				response, err := v.service.Do()
				if err != nil {
					error = err
					log.Err("key is: %s, error: %s", k, err.Error())
					continue
				}

				if response.Errors {
					error = ErrBulkCommit
					log.Err("key is: %s, error: %s", k, ErrBulkCommit.Error())
					continue
				}

				fmt.Println(response.Items)
			}
		} else {
			log.Debug("bulk", "The current time minus " +
						"the past time result is: %d", time)
		}
	}

	return error
}