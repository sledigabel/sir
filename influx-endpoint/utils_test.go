package endpoint_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"
)

func emptyTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//b, _ := ioutil.ReadAll(r.Body)
		ioutil.ReadAll(r.Body)
		//log.Printf("Received: %v\nContent: %v", r, string(b))
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("X-Influxdb-Version", "x.x")
		w.WriteHeader(http.StatusNoContent)
	}))
}
