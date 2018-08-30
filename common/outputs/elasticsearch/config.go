package elasticsearch

type Config struct {
	Hosts  []string
	Size   sizeConfig
	Bulk   bulkConfig
}

type sizeConfig struct {
	Worker  int
	Channel int
	Retries int
}

type bulkConfig struct {
	Size    int
	Timeout int
}

var defaultConfig = Config{}
