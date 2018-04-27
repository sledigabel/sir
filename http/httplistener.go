package httplistener

import (
	"compress/gzip"
	"net"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/models"
)

const (
	defaultAddr string = ":8186"
	defaultRP   string = "autogen"
)

// HTTP is a relay for HTTP influxdb writes
type HTTP struct {
	Addr        string
	Schema      string
	Certificate string
	DefaultRP   string
	Timeout     int

	State    int32
	Listener net.Listener
}

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

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.URL.Path == "/ping" && (r.Method == "GET" || r.Method == "HEAD") {
		w.Header().Add("X-InfluxDB-Version", "relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// TODO
	// if r.URL.Path == "/status" {

	// }

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

	precision := queryParams.Get("precision")
	if precision == "" {
		jsonError(w, http.StatusBadRequest, "missing parameter: precision")
		return
	}

	// parse the points
	// points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	// for now don't do anything
	_, err = models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusBadRequest, "failed parsing points")
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}
