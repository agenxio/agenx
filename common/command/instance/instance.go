package instance

import (
	cryptRand "crypto/rand"
	"flag"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"
	"crypto/md5"
	"io"
	"hash/crc32"

	"github.com/queueio/sentry/utils/component"
	"github.com/queueio/sentry/utils/version"
	"github.com/queueio/sentry/utils/plugin"
	"github.com/queueio/sentry/utils/service"
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/paths"
	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/stats"
)

var instanceDebug = log.MakeDebug("instance")

type Instance struct {
	component.Sentry

	Config     sentryConfig
	RawConfig *config.Config // Raw config that can be unpacked to get Instance specific config data.
}

type sentryConfig struct {
	component.SentryConfig  `config:",inline"`

	ID        int64         `config:"id"`
	Name      string
	Parallel  int

	Evn       string        `config:"env"`
	Host      string
	Version   string

	Path      paths.Path    `config:"path"`
	Stats    *config.Config `config:"stats"`
	Log       log.Logging   `config:"log"`
}

var (
	Version  bool
	setup    bool
)

func init() {
	initRand()
	flag.BoolVar(&Version, "version", false, "Print the version and exit")
}

func initRand() {
	n, err := cryptRand.Int(cryptRand.Reader, big.NewInt(math.MaxInt64))
	seed := n.Int64()
	if err != nil {
		seed = time.Now().UnixNano()
	}

	rand.Seed(seed)
}

func Run(name, version string, f component.Factory) error {
	return handleError(func() error {
		component, err := New(name, version)
		if err != nil {
			return err
		}
		return component.launch(f)
	}())
}

func New(name, ver string) (*Instance, error) {
	if ver == "" {
		ver = version.GetDefaultVersion()
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	hash := md5.New(); io.WriteString(hash, hostname)
	this := component.Sentry{
		Info: component.Info{
			Component: name,
			Version:   ver,
			Name:      hostname,
			Hostname:  hostname,
			ID:        int64(crc32.ChecksumIEEE(hash.Sum(nil)) % 1024),
		},
	}

	return &Instance{Sentry: this}, nil
}

func (i *Instance) Init() error {
	err := i.handleFlags()
	if err != nil {
		return err
	}

	if err := plugin.Initialize(); err != nil {
		return err
	}

	if err := i.configure(); err != nil {
		return err
	}

	return nil
}

func (i *Instance) SentryConfig() (*config.Config, error) {
	name := strings.ToLower(i.Info.Component)
	if i.RawConfig.HasField(name) {
		sub, err := i.RawConfig.Child(name, -1)
		if err != nil {
			return nil, err
		}
		return sub, nil
	}

	return config.New(), nil
}

func (i *Instance) create(f component.Factory) (component.Component, error) {
	configure, err := i.SentryConfig()
	if err != nil {
		return nil, err
	}

	log.Info("Setup Sentry: %s component; Version: %s",
					i.Info.Component, i.Info.Version)

	instanceDebug("Initializing output plugins")
	output, err := plugin.Output(i.Info,  i.Config.Output)
	if err != nil {
		log.Err("error initializing output: %v", err)
	}

	i.Handler = output
	component, err := f(&i.Sentry, configure)
	if err != nil {
		return nil, err
	}

	return component, nil
}

func (i *Instance) launch(f component.Factory) error {
	if err := i.Init(); err != nil {
		return err
	}

	service.Start()
	defer service.Stop()

	sentry, err := i.create(f)
	if err != nil {
		return err
	}

	if config.IsTest() {
		config.Deprecate("6.0", "-test flag has been deprecated, use test sub command")
		fmt.Println("Config OK")
		return component.GracefulExit
	}

	service.HandleSignals(sentry.Stop)

	log.Info("%s start running.", i.Info.Component)
	defer log.Info("%s stopped.", i.Info.Component)

	if i.Config.Stats.Enabled() {
		stats.Start(i.Config.Stats, i.Info)
	}

	return sentry.Run(&i.Sentry)
}

func (i *Instance) TestConfig(f component.Factory) error {
	return handleError(func() error {
		err := i.Init()
		if err != nil {
			return err
		}

		// Create sentry to ensure all settings are OK
		_, err = i.create(f)
		if err != nil {
			return err
		}

		fmt.Println("Config OK")
		return component.GracefulExit
	}())
}

func (i *Instance) Setup(f component.Factory) error {
	return handleError(func() error {
		err := i.Init()
		if err != nil {
			return err
		}
		// Create sentry to give it the opportunity to set loading callbacks
		_, err = i.create(f)
		if err != nil {
			return err
		}

		return nil
	}())
}

func (i *Instance) handleFlags() error {
	err := config.ChangeDefaultCfgFileFlag(i.Info.Component)
	if err != nil {
		return fmt.Errorf("failed to set default config file path: %v", err)
	}
	flag.Parse()

	if Version {
		config.Deprecate("6.0", "-version flag has been Deprecated, use version sub command")
		fmt.Printf("%s version %s (%s), sentry %s\n",
					i.Info.Component, i.Info.Version, runtime.GOARCH, version.GetDefaultVersion())
		return component.GracefulExit
	}

	if err := log.HandleFlags(i.Info.Component); err != nil {
		return err
	}

	return config.HandleFlags()
}

func (i *Instance) configure() error {
	var err error

	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("error loading config file: %v", err)
	}

	i.RawConfig = cfg
	err = cfg.Unpack(&i.Config)
	if err != nil {
		return fmt.Errorf("error unpacking config data: %v", err)
	}

	i.Sentry.Config = &i.Config.SentryConfig

	if id := i.Config.ID; id > 0 {
		i.Info.ID = id
	}

	if name := i.Config.Name; name != "" {
		i.Info.Name = name
	}

	if version := i.Config.Version; version != "" {
		i.Info.Version = version
	}

	err = paths.Init(&i.Config.Path)
	if err != nil {
		return fmt.Errorf("error setting default paths: %v", err)
	}

	log.Info(paths.Paths.String())

	err = log.Init(i.Info.Component, &i.Config.Log)
	if err != nil {
		return fmt.Errorf("error initializing logging: %v", err)
	}

	log.Info("Sentry Instance ID: %v", i.Info.ID)

	if n := i.Config.Parallel; n > 0 {
		runtime.GOMAXPROCS(n)
	}

	i.Sentry.SentryConfig, err = i.SentryConfig()
	if err != nil {
		return err
	}

	return nil
}

func handleError(err error) error {
	if err == nil || err == component.GracefulExit {
		return nil
	}

	log.Critical("Exiting: %v", err)
	return err
}
