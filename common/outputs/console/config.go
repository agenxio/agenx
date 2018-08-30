package console

type Config struct {
	Pretty bool `config:"pretty"`
	Batch  int
}

var defaultConfig = Config{}
