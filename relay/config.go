package relay

import (
	"github.com/sledigabel/sir/httplistener"
	"github.com/sledigabel/sir/influx-endpoint"
)

type Relay struct {
	Listener *httplistener.HTTP
	Backend  *endpoint.HTTPInfluxServerMgr
}

var DefaultConfig string = `
[listener]
	addr = "http://localhost:9090"
	timeout = 5

[backend]
	[server.1]
	alias = test1
`

func NewRelay() *Relay {
	r := Relay{}
	return &r
}

func ParseRelay(s string) (*Relay, error) {
	r := NewRelay()
	httpconfig, err := httplistener.NewHTTPParseConfig(s)
	if err != nil {
		return r, err
	}
	r.Listener = httplistener.NewHTTPfromConfig(httpconfig)
	r.Backend, err = endpoint.NewHTTPInfluxServerMgrFromConfig(s)
	return r, err
}
