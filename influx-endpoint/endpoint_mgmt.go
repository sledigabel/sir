package endpoint

import (
	"errors"
	"fmt"

	"github.com/BurntSushi/toml"
)

// HTTPInfluxServerMgr is the struct
// that manages multiple endpoints
type HTTPInfluxServerMgr struct {
	index     map[string][]*HTTPInfluxServer
	Endpoints map[string]*HTTPInfluxServer
}

// NewHTTPInfluxServerMgr is the constructur
// returns an empty HTTPInfluxServerMgr
func NewHTTPInfluxServerMgr() *HTTPInfluxServerMgr {
	m := HTTPInfluxServerMgr{}
	m.Endpoints = make(map[string]*HTTPInfluxServer)
	m.index = make(map[string][]*HTTPInfluxServer)
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
		if err := s.Connect(); err != nil {
			return err
		}
	}
	return nil
}

// StopAllServers triggers a stop on all
// servers in Endpoints
func (mgr *HTTPInfluxServerMgr) StopAllServers() {
	for _, s := range mgr.Endpoints {
		s.Close()
	}
}
