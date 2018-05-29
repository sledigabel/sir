package relay_test

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/sledigabel/sir/relay"
)

func TestParseRelay(t *testing.T) {

	var testconfig string = `
	[listener]
		addr = "http://localhost:9999"
		timeout = 60
	
	[backend]
		[server.1]
			alias = "test1"
		[server.2]
			alias = "test2"
	`

	r, err := relay.ParseRelay(testconfig)
	if err != nil {
		t.Fatalf("Could not parse basic config: %v", err)
	}
	if r.Listener.Addr != "http://localhost:9999" ||
		r.Listener.Timeout != 60 ||
		len(r.Backend.Endpoints) != 2 ||
		r.Backend.Endpoints["test1"].Alias != "test1" ||
		r.Backend.Endpoints["test2"].Alias != "test2" {

		t.Fatal("Parsing incorrect!")
	}

}

func emptyTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadAll(r.Body)
		log.Printf("Received: %v\nContent: %v", r, string(b))
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("X-Influxdb-Version", "x.x")
	}))
}

func TestEndToEnd(t *testing.T) {

	ts := emptyTestServer()
	defer ts.Close()

	var e2e string = `
	[listener]
		addr = ":8881"
		timeout = 60
	
	[backend]
		[server.1]
		alias = "test"
	`

	r, err := relay.ParseRelay(e2e)
	if err != nil {
		t.Fatalf("Couldn't parse e2e config: %v", err)
	}
	r.Backend.Endpoints["test"].Config.Addr = ts.URL
	r.Listener.BackendMgr = r.Backend

	r.Start()
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8881",
	})
	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "testdb",
		Precision: "s",
	})
	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}

	pt, err := client.NewPoint("cpu_usage", tags, fields, time.Now())
	bp.AddPoint(pt)
	// Write the batch
	if err := c.Write(bp); err != nil {
		t.Fatalf("Error writing point! %v", err)
	}

	r.Stop()
}
