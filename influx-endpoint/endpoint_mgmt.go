package endpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client/v2"
)

// Internal is the struct dedicated
// for collecting statistics
type Internal struct {
	Frequency string
	Database  string
	Enable    bool
}

// HTTPInfluxServerMgr is the struct
// that manages multiple endpoints
type HTTPInfluxServerMgr struct {
	wg        sync.WaitGroup
	Shutdown  chan struct{}
	Telemetry Internal
	Debug     bool
	index     map[string][]*HTTPInfluxServer
	Endpoints map[string]*HTTPInfluxServer
}

// NewHTTPInfluxServerMgr is the constructur
// returns an empty HTTPInfluxServerMgr
func NewHTTPInfluxServerMgr() *HTTPInfluxServerMgr {
	m := HTTPInfluxServerMgr{}
	m.Endpoints = make(map[string]*HTTPInfluxServer)
	m.index = make(map[string][]*HTTPInfluxServer)
	m.Shutdown = make(chan struct{})
	m.Telemetry = Internal{}
	m.Telemetry.Database = "internal"
	m.Telemetry.Frequency = "60s"
	m.Telemetry.Enable = false
	m.Debug = false
	return &m
}

// those types are solely used for the config parsing
type internal Internal
type server HTTPInfluxServerConfig
type servers struct {
	Server   map[string]server
	Internal internal
	Debug    bool
}

// NewHTTPInfluxServerMgrFromConfig is a constructor
// from a toml config file
func NewHTTPInfluxServerMgrFromConfig(config string) (*HTTPInfluxServerMgr, error) {

	m := NewHTTPInfluxServerMgr()
	var e servers

	_, err := toml.Decode(config, &e)
	if err != nil {
		return m, err
	}

	m.Debug = e.Debug
	for _, c := range e.Server {
		// FIXME: horrible type cast
		hc := HTTPInfluxServerConfig(c)
		s := NewHTTPInfluxServerFromConfig(&hc)
		if m.Debug && !hc.Debug {
			s.Debug = true
		}

		if _, ok := m.Endpoints[s.Alias]; ok {
			return m, fmt.Errorf("Error: key %v already exists", s.Alias)
		}
		m.Endpoints[s.Alias] = s
	}

	if e.Internal.Database != "" {
		m.Telemetry.Database = e.Internal.Database
	}
	if e.Internal.Frequency != "" {
		if _, err := time.ParseDuration(e.Internal.Frequency); err != nil {
			log.Fatalf("Unable to parse internal frequency: %v", e.Internal.Frequency)
		}
		m.Telemetry.Frequency = e.Internal.Frequency
	}
	m.Telemetry.Enable = e.Internal.Enable

	return m, nil
}

// GetServerPerName returns the HTTPInfluxServer data
// which alias matches the search string.
func (mgr *HTTPInfluxServerMgr) GetServerPerName(s string) (*HTTPInfluxServer, error) {
	server, ok := mgr.Endpoints[s]
	if ok {
		return server, nil
	}
	return nil, errors.New("Could not find server " + s)
}

// GetInfluxServerbyDB returns the list of Servers
// which regex match the db string
func (mgr *HTTPInfluxServerMgr) GetInfluxServerbyDB(db string) []*HTTPInfluxServer {
	var ret []*HTTPInfluxServer
	for _, server := range mgr.Endpoints {
		for _, reg := range server.Dbregex {
			if match, err := regexp.MatchString(reg, db); match && err == nil {
				ret = append(ret, server)
				break
			}
		}
	}
	return ret
}

// StartAllServers triggers a start for
// all servers in Endpoints
func (mgr *HTTPInfluxServerMgr) StartAllServers() error {
	for _, s := range mgr.Endpoints {
		go func(h *HTTPInfluxServer) {
			mgr.wg.Add(1)
			err := h.Run()
			if err != nil {
				if mgr.Debug {
					log.Printf("An error occured with endpoint %v: %v", h.Alias, err)
				}
			}
			mgr.wg.Done()
		}(s)
	}
	return nil
}

// Stats returns Points which gather up all points
// from all endpoints
func (mgr *HTTPInfluxServerMgr) Stats() (client.BatchPoints, error) {
	batch, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: mgr.Telemetry.Database,
	})
	var err error

	for _, s := range mgr.Endpoints {
		spt, err := s.Stats()
		if err != nil {
			break
		}
		for _, p := range spt {
			batch.AddPoint(client.NewPointFrom(p))
		}
	}
	return batch, err
}

// Status returns a json encoded status check
func (mgr *HTTPInfluxServerMgr) Status() []byte {
	state := make(map[string]string)
	for k, v := range mgr.Endpoints {
		switch v.Status {
		case ServerStateInactive:
			state[k] = "inactive"
		case ServerStateActive:
			state[k] = "active"
		case ServerStateFailed:
			state[k] = "failed"
		case ServerStateStarting:
			state[k] = "starting"
		case ServerStateSuspended:
			state[k] = "suspended"
		case ServerStateDrop:
			state[k] = "drop"
		}
	}
	s, _ := json.Marshal(state)
	return s
}

// StopAllServers triggers a stop on all
// servers in Endpoints
func (mgr *HTTPInfluxServerMgr) StopAllServers() {
	for _, s := range mgr.Endpoints {
		s.Shutdown <- struct{}{}
	}
	mgr.wg.Wait()
}

// Post relays the batch points to the post function
// of each endpoint
func (mgr *HTTPInfluxServerMgr) Post(bp client.BatchPoints) error {
	endpoints, ok := mgr.index[bp.Database()]
	if !ok {
		endpoints = mgr.GetInfluxServerbyDB(bp.Database())
		mgr.index[bp.Database()] = endpoints
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("No endpoint for db %v", bp.Database())
	}
	var err error
	for _, s := range endpoints {

		err = s.Post(bp)
		if err != nil {
			return err
		}
	}
	return err
}

// Run is the main loop
func (mgr *HTTPInfluxServerMgr) Run() {
	if mgr.Debug {
		log.Print("Starting backends")
	}
	err := mgr.StartAllServers()
	if err != nil {
		log.Fatalf("Caught expection while starting servers: %v", err)
	}
	var t *time.Ticker
	if mgr.Telemetry.Enable {
		d, _ := time.ParseDuration(mgr.Telemetry.Frequency)
		t = time.NewTicker(d)
	} else {
		t = &time.Ticker{}
	}

	if mgr.Debug && mgr.Telemetry.Enable {
		log.Printf("collecting stats every %v", mgr.Telemetry.Frequency)
	}

MAINLOOP:
	for {
		select {
		case <-mgr.Shutdown:
			// triggering shutdown
			mgr.StopAllServers()
			mgr.wg.Wait()
			break MAINLOOP

		case <-t.C:
			bp, err := mgr.Stats()
			if err == nil {
				if err2 := mgr.Post(bp); err2 != nil {
					log.Printf("Error posting stats: %v", err2)
				}

			} else {
				log.Printf("Error collecting stats: %v", err)
			}
		}

	}
	if mgr.Debug {
		log.Print("Closing mgr")
	}

}
