package endpoint

import (
	"errors"

	"github.com/BurntSushi/toml"
)

type HTTPInfluxServerMgr struct {
	Endpoints []*HTTPInfluxServer
}

func NewHTTPInfluxServerMgr() *HTTPInfluxServerMgr {
	return &HTTPInfluxServerMgr{}
}

type server HTTPInfluxServerConfig
type servers struct {
	Server map[string]server
}

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
		m.Endpoints = append(m.Endpoints, s)
	}

	return m, nil
}

func (mgr *HTTPInfluxServerMgr) GetServerPerName(s string) (*HTTPInfluxServer, error) {
	return nil, errors.New("Could not find server " + s)
}

func (mgr *HTTPInfluxServerMgr) StartAllServers() error {
	for _, s := range mgr.Endpoints {
		if err := s.Connect(); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *HTTPInfluxServerMgr) StopAllServers() {
	for _, s := range mgr.Endpoints {
		s.Close()
	}
}
