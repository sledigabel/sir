package endpoint

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

// HTTPInfluxServerMgr is the struct
// that manages multiple endpoints
type HTTPInfluxServerMgr struct {
	wg        sync.WaitGroup
	Shutdown  chan struct{}
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
	return &m
}

// those types are solely used for the config parsing
type server HTTPInfluxServerConfig
type servers struct {
	Server map[string]server
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

	for _, c := range e.Server {
		// FIXME: horrible type cast
		hc := HTTPInfluxServerConfig(c)
		s := NewHTTPInfluxServerFromConfig(&hc)

		if _, ok := m.Endpoints[s.Alias]; ok {
			return m, fmt.Errorf("Error: key %v already exists", s.Alias)
		}
		m.Endpoints[s.Alias] = s
	}

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
				log.Printf("An error occured with endpoint %v: %v", h.Alias, err)
			}
			mgr.wg.Done()
		}(s)
	}
	return nil
}

// Stats returns Points which gather up all points
// from all endpoints
func (mgr *HTTPInfluxServerMgr) Stats() (models.Points, error) {
	pts := make(models.Points, len(mgr.Endpoints))
	var err error
	for _, s := range mgr.Endpoints {
		pt, err := s.Stats()
		if err != nil {
			break
		}
		pts = append(pts, pt)
	}
	return pts, err
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

	err := mgr.StartAllServers()
	if err != nil {
		log.Printf("Caught expection while starting servers: %v", err)
	}

MAINLOOP:
	for {
		select {
		case <-mgr.Shutdown:
			// triggering shutdown
			mgr.StopAllServers()
			mgr.wg.Wait()
			break MAINLOOP
		}
	}
	log.Print("Closing mgr")

}
