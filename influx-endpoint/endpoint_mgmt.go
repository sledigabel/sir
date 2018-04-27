package endpoint

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/BurntSushi/toml"
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

// StartAllServers triggers a start for
// all servers in Endpoints
func (mgr *HTTPInfluxServerMgr) StartAllServers() error {
	for _, s := range mgr.Endpoints {
		mgr.wg.Add(1)
		go s.Run(&mgr.wg)
	}
	return nil
}

// StopAllServers triggers a stop on all
// servers in Endpoints
func (mgr *HTTPInfluxServerMgr) StopAllServers() {
	for _, s := range mgr.Endpoints {
		s.Shutdown <- struct{}{}
	}
	mgr.wg.Wait()
}

// Run is the main loop
func (mgr *HTTPInfluxServerMgr) Run(wg *sync.WaitGroup) {

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
	wg.Done()

}
