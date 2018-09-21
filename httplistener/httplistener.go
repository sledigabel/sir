package httplistener

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
)

const (
	defaultAddr string = ":8186"
	defaultRP   string = "autogen"
)

// Backend represents a backend
// entity to relay metrics to
type Backend interface {
	Post(client.BatchPoints) error
	Status() []byte
}

// HTTP is a relay for HTTP influxdb writes
type HTTP struct {
	Addr        string
	Schema      string
	Certificate string
	DefaultRP   string
	Timeout     int

	State            int32
	Listener         net.Listener
	Debug            bool
	DebugConnections bool
	BackendMgr       Backend
}

// HTTPConf is the basic config structure for HTTP
type HTTPConf struct {
	Addr             string `toml:"addr"`
	Certificate      string
	RetentionPolicy  string `toml:"retention_policy"`
	Timeout          int
	Debug            bool
	DebugConnections bool `toml:"log"`
}

type responseData struct {
	ContentType     string
	ContentEncoding string
	StatusCode      int
	Body            []byte
}

// NewHTTP is the builder for HTTP
func NewHTTP() *HTTP {
	// arbitrary values
	h := &HTTP{
		Addr:             "localhost:8186",
		Certificate:      "",
		Timeout:          60,
		Debug:            false,
		DebugConnections: false,
	}
	return h
}

// NewHTTPWithParameters is a parameterised builder for HTTP
func NewHTTPWithParameters(addr string, cert string, rp string, timeout int) *HTTP {
	if rp == "" {
		rp = defaultRP
	}
	return &HTTP{
		Addr:             addr,
		Certificate:      cert,
		DefaultRP:        rp,
		Timeout:          timeout,
		Debug:            false,
		DebugConnections: false,
	}
}

func NewHTTPfromConfig(hc *HTTPConf) *HTTP {
	h := NewHTTPWithParameters(hc.Addr, hc.Certificate, hc.RetentionPolicy, hc.Timeout)
	h.Debug = hc.Debug
	h.DebugConnections = hc.DebugConnections
	return h
}

type listener HTTPConf
type myconf struct {
	Listener listener
}

func NewHTTPParseConfig(conf string) (*HTTPConf, error) {
	l := myconf{}
	_, err := toml.Decode(conf, &l)
	hc := HTTPConf(l.Listener)
	return &hc, err
}

func (h *HTTP) toString() string {
	if h.Certificate != "" {
		return fmt.Sprintf("https://%v", h.Addr)
	}
	return fmt.Sprintf("http://%v", h.Addr)
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if h.DebugConnections {
		log.Printf("Connection: %v [%v %v] (%v)", r.RemoteAddr, r.Method, r.URL.RequestURI(), r.ContentLength)
	}

	if r.URL.Path == "/ping" && (r.Method == "GET" || r.Method == "HEAD") {
		w.Header().Add("X-InfluxDB-Version", "relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.URL.Path == "/query" {
		if r.Method != "GET" {
			w.Header().Add("X-InfluxDB-Version", "relay")
			jsonError(w, http.StatusForbidden, "Queries supported only for SELECT and SHOW")
			return
		}
		w.Header().Add("X-InfluxDB-Version", "relay")
		// saving the query and passing it along
		queryParams := r.URL.Query()
		db := queryParams.Get("db")
		if db == "" {
			jsonError(w, http.StatusBadRequest, "missing parameter: db")
			return
		}
		bd, _ := ioutil.ReadAll(r.Body)
		log.Printf("Params:\n- db: %v\n- raw: %v\n- Body: %v", db, r.URL.RawQuery, string(bd))
		log.Printf("Host: %v = %v", r.Host, r.Header)
		r.Host = "127.0.0.1:19998"
		log.Printf("Host: %v = %v", r.Host, r.Header)
		queryRelay := &http.Client{}
		u, _ := url.ParseRequestURI("http://127.0.0.1:19998")
		body, _ := ioutil.ReadAll(r.Body)
		newReq := &http.Request{
			Method:        r.Method,
			URL:           u,
			Body:          newBody(body),
			Header:        make(http.Header),
			ContentLength: int64(len(body)),
		}
		for name, headers := range r.Header {
			for _, h := range headers {
				fmt.Printf("%v: %v\n", name, h)
				newReq.Header.Set(name, h)
			}
		}
		// resp, err := queryRelay.Do(r)
		queryResp, err := queryRelay.Do(newReq)
		log.Printf("error: %v", err)
		log.Printf("%v / %v", queryResp.StatusCode, err)
		// if err != nil {
		// 	jsonError(w, http.StatusInternalServerError, "Couldn't reach server")
		// }
		// w.Write([]byte(resp.Status))
		// w.WriteHeader(resp.StatusCode)
		return
	}

	if r.URL.Path == "/status" && r.Method == "GET" {
		w.Header().Add("X-InfluxDB-Version", "relay")
		w.WriteHeader(http.StatusOK)
		if h.BackendMgr != nil {
			w.Write(h.BackendMgr.Status())
		} else {
			w.Write([]byte("{}"))
		}
		return
	}

	// we only accept writes
	if r.URL.Path != "/write" {
		jsonError(w, http.StatusNotFound, "invalid endpoint")
		return
	}

	queryParams := r.URL.Query()

	// fail early if we're missing the database
	if queryParams.Get("db") == "" {
		jsonError(w, http.StatusBadRequest, "missing parameter: db")
		return
	}
	// override RP if not specified
	if queryParams.Get("rp") == "" {
		if h.DefaultRP == "" {
			h.DefaultRP = defaultRP
		}
		queryParams.Set("rp", h.DefaultRP)
	}

	// gzip compatible
	var body = r.Body

	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
		}
		defer b.Close()
		body = b
	}

	// read from Body
	bodyBuf := getBuf()
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusInternalServerError, "Failed reading request body")
		return
	}

	// the default would be nanosecond if precision isn't specified.
	precision := queryParams.Get("precision")

	// prep the batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  queryParams.Get("db"),
		Precision: precision,
	})

	// parse the points
	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusBadRequest, "failed parsing points")
		return
	}

	for _, p := range points {
		bp.AddPoint(client.NewPointFrom(p))
	}

	// if we have a backend configured, post
	if h.BackendMgr != nil {
		err = h.BackendMgr.Post(bp)
		if err != nil {
			jsonError(w, http.StatusServiceUnavailable, err.Error())
		}
	}

	w.WriteHeader(http.StatusNoContent)
	w.Header().Add("X-InfluxDB-Version", "relay")

	return
}
