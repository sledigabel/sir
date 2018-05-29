package endpoint

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

// ServerState events
const (
	ServerStateInactive  uint32        = 0
	ServerStateStarting  uint32        = 1
	ServerStateActive    uint32        = 2
	ServerStateSuspended uint32        = 3
	ServerStateFailed    uint32        = 4
	ServerStateDrop      uint32        = 5
	UserAgent            string        = "SIR"
	DefaultPrintFreq     time.Duration = 10 * time.Second
)

// HTTPInfluxServer type
type HTTPInfluxServer struct {
	Alias       string
	Dbregex     []string
	Client      client.Client
	Status      uint32
	Config      *client.HTTPConfig
	Shutdown    chan struct{}
	concurrent  chan struct{}
	PingFreq    time.Duration
	NumRq       uint
	PostCounter uint64
}

// NewHTTPInfluxServer is a
// constructor of HTTPInfluxServer
func NewHTTPInfluxServer(alias string, dbregex []string, httpConfig *client.HTTPConfig) (*HTTPInfluxServer, error) {

	if len(alias) == 0 {
		return &HTTPInfluxServer{}, errors.New("Alias cannot be empty")
	}
	if len(dbregex) == 0 {
		dbregex = []string{".*"}
	}

	// adding the user agent if empty
	if len(httpConfig.UserAgent) == 0 {
		httpConfig.UserAgent = UserAgent
	}

	return &HTTPInfluxServer{
		Alias:      alias,
		Dbregex:    dbregex,
		Config:     httpConfig,
		Status:     ServerStateActive,
		Shutdown:   make(chan struct{}),
		NumRq:      100,
		PingFreq:   10 * time.Second,
		concurrent: make(chan struct{}, 100),
	}, nil
}

// Connect triggers the connection to the database
func (server *HTTPInfluxServer) Connect() error {
	c, err := client.NewHTTPClient(*server.Config)
	if err != nil {
		log.Panic(err)
		atomic.StoreUint32(&server.Status, ServerStateFailed)
	} else {
		server.Client = c
		atomic.StoreUint32(&server.Status, ServerStateActive)
	}
	return err
}

// Close is the closing helper
func (server *HTTPInfluxServer) Close() {
	// Close is idempotent; only actually close when it needs to
	if server.Client != nil {
		server.Client.Close()
	}
	atomic.StoreUint32(&server.Status, ServerStateInactive)
}

// helps with the toml parsing
type duration struct {
	time.Duration
}

func (d duration) toTimeDuration() time.Duration {
	return time.Duration(d.Duration)
}

// HTTPInfluxServerConfig is the struct to
// map influx servers from config items
type HTTPInfluxServerConfig struct {
	ServerName       string `toml:"server_name"`
	Alias            string
	DBregex          []string `toml:"db_regex"`
	Username         string
	Password         string
	Precision        string
	WriteConsistency string `toml:"write_consistency"`
	Port             int
	Timeout          duration
	UnsafeSSL        bool `toml:"unsafe_ssl"`
	Secure           bool
	Disable          bool     `toml:"disable"`
	ConcurrentRq     int      `toml:"max_concurrent_requests"`
	PingFrequency    duration `toml:"ping_frequency"`
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// NewHTTPInfluxServerParseConfig creates a new
// config from config
func NewHTTPInfluxServerParseConfig(config string) (*HTTPInfluxServerConfig, error) {
	var conf HTTPInfluxServerConfig
	_, err := toml.Decode(config, &conf)
	return &conf, err
}

// NewHTTPInfluxServerFromConfig creates a
// server from a given config struct
func NewHTTPInfluxServerFromConfig(c *HTTPInfluxServerConfig) *HTTPInfluxServer {
	new := &HTTPInfluxServer{}
	new.Alias = c.Alias
	if len(c.DBregex) == 0 {
		new.Dbregex = []string{".*"}
	} else {
		new.Dbregex = c.DBregex
	}
	if c.Disable {
		atomic.StoreUint32(&new.Status, ServerStateSuspended)
	}
	new.Config = &client.HTTPConfig{}
	if c.Secure {
		new.Config.Addr = "https://"
	} else {
		new.Config.Addr = "http://"
	}
	new.Config.Addr = fmt.Sprintf("%s%s:%d", new.Config.Addr, c.ServerName, c.Port)
	new.Config.InsecureSkipVerify = c.UnsafeSSL
	new.Config.Username = c.Username
	new.Config.Password = c.Password
	new.Config.Timeout = c.Timeout.toTimeDuration()
	new.Shutdown = make(chan struct{})
	if c.ConcurrentRq > 0 {
		new.NumRq = uint(c.ConcurrentRq)
	} else {
		new.NumRq = 100
	}
	new.concurrent = make(chan struct{}, new.NumRq)
	new.PingFreq = c.PingFrequency.toTimeDuration()
	if new.PingFreq == 0 {
		new.PingFreq = DefaultPrintFreq
	}

	return new
}

// Ping returns nil if the connection is successful
// and a No content ping is made
// otherwise, propagates the error and sets the state.
func (server *HTTPInfluxServer) Ping() error {
	state := atomic.LoadUint32(&server.Status)
	if state == ServerStateInactive || server.Client == nil {
		return errors.New("Server connection not initialised")
	}
	_, _, err := server.Client.Ping(server.Config.Timeout)
	if err != nil && (state == ServerStateActive || state == ServerStateDrop) {
		atomic.StoreUint32(&server.Status, ServerStateFailed)
	} else {
		if state == ServerStateFailed {
			atomic.StoreUint32(&server.Status, ServerStateActive)
		}
	}
	return err
}

// Stats return a data point per relay
func (server *HTTPInfluxServer) Stats() (models.Point, error) {
	tags := models.NewTags(map[string]string{"alias": server.Alias})
	fields := map[string]interface{}{
		"active_req": len(server.concurrent),
		"state":      atomic.LoadUint32(&server.Status),
		"count":      atomic.LoadUint64(&server.PostCounter),
	}

	return models.NewPoint("sir_relay", tags, fields, time.Now())
}

// Post is the main posting function.
// Returns nil if all good, otherwise error.
func (server *HTTPInfluxServer) Post(bp client.BatchPoints) error {
	server.concurrent <- struct{}{}
	err := server.Client.Write(bp)
	if err != nil {
		log.Printf("Couldn't post to Influx server %v: %v", server.Alias, err)
		// TODO: something smart
	} else {
		atomic.AddUint64(&server.PostCounter, uint64(len(bp.Points())))
	}
	<-server.concurrent
	// at the moment, pass the post err as is
	return err
}

// Run is the main loop
func (server *HTTPInfluxServer) Run() error {

	if atomic.LoadUint32(&server.Status) != ServerStateSuspended {
		atomic.StoreUint32(&server.Status, ServerStateStarting)
		server.Connect()
	}

	tick := time.NewTicker(server.PingFreq)
	// TODO: Add good start and watchdog logic

MAINLOOP:
	for {
		select {
		case <-server.Shutdown:
			// triggering shutdown
			log.Printf("Received shutdown for server %v", server.Alias)
			server.Close()
			break MAINLOOP

		case <-tick.C:
			log.Printf("Tick for server %v\n", server.Alias)
			server.Ping()
		}
	}

	return nil
}
