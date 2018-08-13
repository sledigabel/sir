package endpoint

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"
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
	Alias           string
	Dbregex         []string
	Client          client.Client
	Status          uint32
	Config          *client.HTTPConfig
	Shutdown        chan struct{}
	concurrent      chan struct{}
	PingFreq        time.Duration
	NumRq           uint
	PostCounter     uint64
	DbCounters      map[string]uint64
	DbCountersMutex sync.Mutex
	Debug           bool
	Buffering       bool
	Bufferer        *Bufferer
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
		Alias:           alias,
		Dbregex:         dbregex,
		Config:          httpConfig,
		Status:          ServerStateActive,
		Shutdown:        make(chan struct{}),
		NumRq:           100,
		PingFreq:        10 * time.Second,
		concurrent:      make(chan struct{}, 100),
		Debug:           false,
		DbCounters:      make(map[string]uint64, 0),
		DbCountersMutex: sync.Mutex{},
		Buffering:       false,
		Bufferer:        NewBufferer(),
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
	ServerName        string `toml:"server_name"`
	Alias             string
	DBregex           []string `toml:"db_regex"`
	Username          string
	Password          string
	Precision         string
	WriteConsistency  string `toml:"write_consistency"`
	Port              int
	Timeout           duration
	UnsafeSSL         bool `toml:"unsafe_ssl"`
	Secure            bool
	Disable           bool     `toml:"disable"`
	ConcurrentRq      int      `toml:"max_concurrent_requests"`
	PingFrequency     duration `toml:"ping_frequency"`
	Debug             bool     `toml:"debug"`
	Buffering         bool     `toml:"buffering"`
	BufferPath        string   `toml:"buffer_path"`
	BufferFlushFreq   duration `toml:"buffer_flush_frequency"`
	BufferCompression bool     `toml:"buffer_compression"`
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
	new.Config.UserAgent = UserAgent
	if c.Secure {
		new.Config.Addr = "https://"
	} else {
		new.Config.Addr = "http://"
	}
	new.Config.Addr = fmt.Sprintf("%s%s:%d", new.Config.Addr, c.ServerName, c.Port)
	new.Config.InsecureSkipVerify = c.UnsafeSSL
	new.Config.Username = c.Username
	new.Config.Password = c.Password
	if c.Timeout.Duration.String() != "0s" {
		new.Config.Timeout = c.Timeout.toTimeDuration()
	} else {
		new.Config.Timeout, _ = time.ParseDuration("30s")
	}
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
	new.DbCounters = make(map[string]uint64)
	new.DbCountersMutex = sync.Mutex{}
	new.Debug = c.Debug
	new.Buffering = c.Buffering
	if new.Buffering {
		new.Bufferer = NewBufferer()
		if c.BufferPath != "" {
			new.Bufferer.RootPath = filepath.Join(c.BufferPath, new.Alias)
		} else {
			new.Bufferer.RootPath = new.Alias
		}
		if c.BufferFlushFreq.Duration.String() != "0s" {
			new.Bufferer.FlushFrequency = c.BufferFlushFreq.Duration
		} else {
			new.Bufferer.FlushFrequency, _ = time.ParseDuration("10s")
		}
		new.Bufferer.Compression = c.BufferCompression
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
		log.Printf("Check for server %v failed (%v)", server.Alias, err)
		atomic.StoreUint32(&server.Status, ServerStateFailed)
	}
	if err == nil && state != ServerStateActive {
		log.Printf("Check successful for server %v", server.Alias)
		atomic.StoreUint32(&server.Status, ServerStateActive)
	}
	return err
}

// Stats return a data point per relay
func (server *HTTPInfluxServer) Stats() ([]models.Point, error) {
	pts := make([]models.Point, 0)
	tags := models.NewTags(map[string]string{
		"alias": server.Alias,
	})
	server.DbCountersMutex.Lock()
	defer server.DbCountersMutex.Unlock()
	fields := map[string]interface{}{
		"active_req": len(server.concurrent),
		"state":      int(atomic.LoadUint32(&server.Status)),
		"posted":     int64(server.PostCounter),
	}

	pt, _ := models.NewPoint("sir_backend", tags, fields, time.Now())
	pts = append(pts, pt)
	for db, count := range server.DbCounters {
		tags = models.NewTags(map[string]string{
			"alias":    server.Alias,
			"database": db})
		fields = map[string]interface{}{
			"posted": int64(count),
		}
		pt, _ = models.NewPoint("sir_db", tags, fields, time.Now())
		pts = append(pts, pt)
	}
	if server.Buffering {
		bpt, _ := server.Bufferer.Stats()
		for _, p := range bpt {
			p.AddTag("alias", server.Alias)
			pts = append(pts, p)
		}
	}
	return pts, nil
}

// _port is the internal main posting function.
// Returns nil if all good, otherwise error.
func (server *HTTPInfluxServer) _post(bp client.BatchPoints) error {
	server.concurrent <- struct{}{}
	// TODO: manage conditional state
	err := server.Client.Write(bp)
	if err != nil {
		if server.Debug {
			log.Printf("Couldn't post to Influx server %v: %v", server.Alias, err)
		}
		server.Ping()
		return err
	}
	server.DbCountersMutex.Lock()
	server.PostCounter += uint64(len(bp.Points()))
	if _, ok := server.DbCounters[bp.Database()]; !ok {
		server.DbCounters[bp.Database()] = uint64(len(bp.Points()))
	} else {
		server.DbCounters[bp.Database()] += uint64(len(bp.Points()))
	}
	server.DbCountersMutex.Unlock()

	<-server.concurrent
	// at the moment, pass the post err as is
	return err
}

// Post is a wrapper around internal _post,
// allowing smarter decision making.
func (server *HTTPInfluxServer) Post(bp client.BatchPoints) error {

	if atomic.LoadUint32(&server.Status) != ServerStateActive {
		if server.Buffering {
			server.Bufferer.Input <- bp
			return nil
		}
		return fmt.Errorf("Server %v is not active", server.Alias)
	}
	err := server._post(bp)
	if err != nil && server.Buffering {
		server.Bufferer.Input <- bp
		return nil
	}
	return err

}

// ProcessBacklog will run and periodically process the backlog
// of batches that are written to disk.
func (server *HTTPInfluxServer) ProcessBacklog(stop chan struct{}) error {
	// processing 40 per minute by default
	backlog := time.NewTicker(15 * time.Millisecond)
LOOP:
	for {
		select {
		case <-stop:
			break LOOP
		case <-backlog.C:
			if server.Status == ServerStateActive {
				bp, err := server.Bufferer.Pop()
				if err != nil {
					return err
				}
				if bp != nil {
					if err = server.Post(bp); err != nil {
						return err
					}
				}
			}
		}

	}
	return nil
}

// Run is the main loop
func (server *HTTPInfluxServer) Run() error {

	var bufferwg sync.WaitGroup
	bufferbacklog := make(chan struct{})
	if server.Buffering {
		if err := server.Bufferer.Init(); err != nil {
			log.Fatalf("Could not initialise server %v: %v", server.Alias, err)
		}
		go func() {
			bufferwg.Add(1)
			err := server.Bufferer.Run()
			if err != nil {
				log.Fatalf("Bufferer for server %v failed: %v", server.Alias, err)
			}
			bufferwg.Done()
		}()
		go func() {
			bufferwg.Add(1)
			err := server.ProcessBacklog(bufferbacklog)
			if err != nil {
				log.Fatalf("Bufferer for server %v failed: %v", server.Alias, err)
			}
			bufferwg.Done()
		}()
	}

	if atomic.LoadUint32(&server.Status) != ServerStateSuspended {
		atomic.StoreUint32(&server.Status, ServerStateStarting)
		if err := server.Connect(); err != nil {
			atomic.StoreUint32(&server.Status, ServerStateFailed)
			return err
		}
		if err := server.Ping(); err != nil {
			log.Printf("Error while connecting to server %v: %v", server.Alias, err)
		}
	}

	tick := time.NewTicker(server.PingFreq)

MAINLOOP:
	for {
		select {
		case <-server.Shutdown:
			// triggering shutdown
			if server.Debug {
				log.Printf("Received shutdown for server %v", server.Alias)
			}
			server.Close()
			if server.Buffering {
				server.Bufferer.Shutdown <- struct{}{}
				bufferbacklog <- struct{}{}
				bufferwg.Wait()
			}
			break MAINLOOP

		case <-tick.C:
			state := atomic.LoadUint32(&server.Status)
			if state != ServerStateSuspended {
				server.Ping()
			}
		}
	}

	return nil
}
