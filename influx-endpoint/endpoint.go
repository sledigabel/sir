package endpoint

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client/v2"
)

// ServerState is a set of constant states
type ServerState uint8

// ServerState events
const (
	ServerStateInactive  ServerState = 0
	ServerStateActive    ServerState = 1
	ServerStateSuspended ServerState = 2
	UserAgent            string      = "SIR"
)

// HTTPInfluxServer type
type HTTPInfluxServer struct {
	Alias   string
	Dbregex string
	Client  client.Client
	Status  ServerState
	Config  *client.HTTPConfig
}

// NewHTTPInfluxServer is a
// constructor of HTTPInfluxServer
func NewHTTPInfluxServer(alias string, dbregex string, httpConfig *client.HTTPConfig) (*HTTPInfluxServer, error) {

	if len(alias) == 0 {
		return &HTTPInfluxServer{}, errors.New("Alias cannot be empty")
	}
	if len(dbregex) == 0 {
		dbregex = ".*"
	}

	// adding the user agent if empty
	if len(httpConfig.UserAgent) == 0 {
		httpConfig.UserAgent = UserAgent
	}

	return &HTTPInfluxServer{
		Alias:   alias,
		Dbregex: dbregex,
		Config:  httpConfig,
		Status:  ServerStateActive,
	}, nil
}

// Connect triggers the connection to the database
func (server *HTTPInfluxServer) Connect() error {
	c, err := client.NewHTTPClient(*server.Config)
	if err != nil {
		log.Panic(err)
		server.Status = ServerStateInactive
	} else {
		server.Client = c
		server.Status = ServerStateActive
	}
	return err
}

// Close is the closing helper
func (server *HTTPInfluxServer) Close() {
	server.Client.Close()
	server.Status = ServerStateInactive
}

// GetInfluxServerbyDB returns the list of Servers
// which regex match the db string
func GetInfluxServerbyDB(db string, servers []*HTTPInfluxServer) []*HTTPInfluxServer {
	var ret []*HTTPInfluxServer
	for _, server := range servers {
		if match, err := regexp.MatchString(server.Dbregex, db); match && err == nil {
			ret = append(ret, server)
		}
	}
	return ret
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
	DBregex          string `toml:"db_regex"`
	Username         string
	Password         string
	Precision        string
	WriteConsistency string `toml:"write_consistency"`
	Port             int
	Timeout          duration
	UnsafeSSL        bool `toml:"unsafe_ssl"`
	Secure           bool
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// NewHTTPInfluxServerFromConfig creates a new
// config from config
func NewHTTPInfluxServerParseConfig(config string) (*HTTPInfluxServerConfig, error) {
	var conf HTTPInfluxServerConfig
	_, err := toml.Decode(config, &conf)
	return &conf, err
}

func NewHTTPInfluxServerFromConfig(c *HTTPInfluxServerConfig) *HTTPInfluxServer {
	new := &HTTPInfluxServer{}
	new.Alias = c.Alias
	new.Dbregex = c.DBregex
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

	return new
}