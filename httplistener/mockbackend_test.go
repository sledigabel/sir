package httplistener_test

import (
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/sledigabel/sir/httplistener"
)

// MockBE is a backend that stores all points
// in an array
type MockBE struct {
	Databases []string
	Points    []client.Point
}

func NewMockBE() *MockBE {
	mbe := MockBE{
		Databases: make([]string, 0),
		Points:    make([]client.Point, 0),
	}
	return &mbe
}

func (mbe *MockBE) Post(bp client.BatchPoints) error {
	var db_exist bool
	for _, v := range mbe.Databases {
		if v == bp.Database() {
			db_exist = true
		}
	}
	if !db_exist {
		mbe.Databases = append(mbe.Databases, bp.Database())
	}

	for _, p := range bp.Points() {
		mbe.Points = append(mbe.Points, *p)
	}

	return nil
}

func TestSubmitData(t *testing.T) {

	t.Log("Starting server")
	h := httplistener.NewHTTP()
	h.Addr = "localhost:19999"
	m := NewMockBE()
	h.BackendMgr = m
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		err := h.Run()

		if err != nil {
			t.Fatalf("Found an error with server: %v", err)
		}
		wg.Done()
	}()

	time.Sleep(time.Second)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:19999",
	})
	if err != nil {
		t.Fatalf("Cannot connect to the server using influx client")
	}

	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: "testdb",
	})

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}
	pt, _ := client.NewPoint("cpu_usage", tags, fields, time.Now())
	bp.AddPoint(pt)
	c.Write(bp)

	t.Logf("Currently having %v points\n", len(m.Points))
	if len(m.Points) != 1 || len(m.Databases) != 1 || m.Databases[0] != "testdb" {
		t.Fatalf("Error, point not received by mock back end.")
	}
	t.Log("Stopping server")
	h.Stop()
	wg.Wait()

}
