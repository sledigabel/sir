package httplistener

import (
	"compress/gzip"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

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
	Addr            string
	Certificate     string
	RetentionPolicy string
	Timeout         int
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

	if r.URL.Path == "/query" && r.Method == "GET" {
		jsonError(w, http.StatusForbidden, "queries not allowed")
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
			// TODO: something smart
		}
	}

	w.WriteHeader(http.StatusNoContent)
	w.Header().Add("X-InfluxDB-Version", "relay")
	return
}
