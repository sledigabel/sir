package endpoint

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/segmentio/ksuid"
)

var bufferSizeMax = 10000

// BufferFile is the struct describing a batch as a buffer
type BufferFile struct {
	Filename        string `json:"filename"`
	NumMetrics      int    `json:"num_metrics"`
	Database        string `json:"database"`
	RetentionPolicy string `json:"retention_policy"`
	Precision       string `json:"precision"`
}

// Bufferer is the main buffering struct
type Bufferer struct {
	Input          chan client.BatchPoints
	Output         chan client.BatchPoints
	Index          []*BufferFile
	RootPath       string
	FlushFrequency time.Duration
	Shutdown       chan struct{}
	Lock           sync.Mutex
}

// BatchBuffer is the marshalling struct for batches
type BatchBuffer struct {
	Database        string `json:"database"`
	RetentionPolicy string `json:"retention_policy"`
	Precision       string `json:"precision"`
	Points          string `json:"points"`
}

// NewBatchBufferFromBP creates a BatchBuffer from a batch
func NewBatchBufferFromBP(bp client.BatchPoints) *BatchBuffer {
	bb := BatchBuffer{
		Database:        bp.Database(),
		Precision:       bp.Precision(),
		RetentionPolicy: bp.RetentionPolicy(),
	}
	for i, p := range bp.Points() {
		if i == 0 {
			bb.Points = p.String()
		} else {
			bb.Points = fmt.Sprintf("%s\n%s", bb.Points, p.String())
		}
	}
	return &bb
}

// BatchPoints converts a BatchBuffer into a batch
func (b *BatchBuffer) BatchPoints() (client.BatchPoints, error) {

	bpc := client.BatchPointsConfig{
		Precision:       b.Precision,
		Database:        b.Database,
		RetentionPolicy: b.RetentionPolicy,
	}
	bp, err := client.NewBatchPoints(bpc)
	if err != nil {
		return bp, err
	}
	var pts []models.Point
	pts, err = models.ParsePointsString(b.Points)
	for _, p := range pts {
		bp.AddPoint(client.NewPointFrom(p))
	}
	return bp, err
}

// NewBufferFile instantiate a BufferFile
func NewBufferFile() *BufferFile {
	return &BufferFile{}
}

// NewBufferFileFromBP creates a BufferFile from a batch
func NewBufferFileFromBP(bp client.BatchPoints) *BufferFile {

	id := ksuid.New()
	bf := BufferFile{
		Database:        bp.Database(),
		RetentionPolicy: bp.RetentionPolicy(),
		Precision:       bp.Precision(),
		NumMetrics:      len(bp.Points()),
		Filename:        id.String(),
	}

	return &bf
}

func (bf *BufferFile) String() string {
	return fmt.Sprintf("%s-%s-%s", bf.Database, bf.RetentionPolicy, bf.Precision)
}

// NewBufferer creates a new Buffer
func NewBufferer() *Bufferer {
	b := Bufferer{}
	b.Index = make([]*BufferFile, 0)
	b.RootPath, _ = os.Getwd()
	b.Input = make(chan client.BatchPoints, bufferSizeMax)
	b.FlushFrequency = 5 * time.Minute
	b.Shutdown = make(chan struct{})
	return &b
}

// BufferSave is the marshalling struct for Bufferers
type BufferSave struct {
	Index []*BufferFile `json:"bufferfiles"`
}

// SaveIndex marshalls a buffer into a file
func (b *Bufferer) SaveIndex() error {

	bs := BufferSave{
		Index: b.Index,
	}
	bytes, err := json.Marshal(bs)
	if err != nil {
		return err
	}
	fd, err := os.Create(filepath.Join(b.RootPath, "index.json"))
	if err != nil {
		return err
	}
	defer fd.Close()
	fd.Write(bytes)
	return nil
}

// LoadIndex loads up a previously saved Buffer and unmarshalls it
func (b *Bufferer) LoadIndex() error {
	var bs BufferSave
	if _, err := os.Stat(filepath.Join(b.RootPath, "index.json")); err != nil {
		b.Index = make([]*BufferFile, 0)
		return nil
	}
	fd, err := os.Open(filepath.Join(b.RootPath, "index.json"))
	if err != nil {
		return err
	}
	bytes, err := ioutil.ReadAll(fd)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(bytes, &bs); err != nil {
		return err
	}
	b.Index = bs.Index
	// successful load, deleting index.json
	os.Remove(filepath.Join(b.RootPath, "index.json"))
	return nil

}

// Init creates the Bufferer directory if needed and tests
// if it can write into it, otherwise throws an error.
// It will load any existing saved Buffer if found.
func (b *Bufferer) Init() error {

	// create dir if needs to
	if _, err := os.Stat(b.RootPath); os.IsNotExist(err) {
		err := os.MkdirAll(b.RootPath, 0755)
		if err != nil {
			return err
		}
	}

	// test write access at start time
	fp := filepath.Join(b.RootPath, "dummy.txt")
	f, err := os.Create(fp)
	f.Write([]byte{' '})
	f.Close()
	defer os.Remove(fp)

	// insert here all the magic to recover from stop
	err = b.LoadIndex()
	if err != nil {
		return fmt.Errorf("Unable to load index: %v", err)
	}

	return err

}

func (b *Bufferer) Write(bp client.BatchPoints) error {
	b.Lock.Lock()
	defer b.Lock.Unlock()
	bf := NewBufferFileFromBP(bp)
	f, err := os.Create(filepath.Join(b.RootPath, bf.Filename))
	if err != nil {
		return err
	}
	defer f.Close()

	bb := NewBatchBufferFromBP(bp)
	content, err := json.Marshal(bb)
	if err != nil {
		return err
		// TODO: test and handle marshalling errors and delete file
	}
	_, err = f.Write(content)
	if err != nil {
		// again handle here file delete
		return err
	}

	b.Index = append(b.Index, bf)
	return nil
}

// Flush triggers a Bufferer to write to disk
// All the batches from the Input channel will be
// flushed into BufferFiles and written to disk
func (b *Bufferer) Flush() error {

	batches := make(map[string]client.BatchPoints)

EMPTYCHANNEL:
	for {
		select {
		case bp := <-b.Input:
			ind := fmt.Sprintf("%s%s%s", bp.Database(), bp.RetentionPolicy(), bp.Precision())
			if _, ok := batches[ind]; ok {
				for _, p := range bp.Points() {
					batches[ind].AddPoint(p)
				}
			} else {
				batches[ind] = bp
			}
		default:
			break EMPTYCHANNEL
		}
	}

	for _, batch := range batches {
		if err := b.Write(batch); err != nil {
			return err
		}
	}
	return nil
}

// Pop returns the first (oldest) element of the index
func (b *Bufferer) Pop() (client.BatchPoints, error) {

	b.Lock.Lock()
	defer b.Lock.Unlock()
	if len(b.Index) == 0 {
		return nil, nil
	}

	bb := BatchBuffer{}
	fd, err := os.Open(filepath.Join(b.RootPath, b.Index[0].Filename))
	if err != nil {
		return nil, err
	}
	bf, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}
	fd.Close()

	if err = json.Unmarshal(bf, &bb); err != nil {
		return nil, err
	}

	bp, err := bb.BatchPoints()
	if err != nil {
		return nil, err
	}

	err = os.Remove(filepath.Join(b.RootPath, b.Index[0].Filename))
	b.Index = b.Index[1:]
	return bp, err

}

// Run is the main function
func (b *Bufferer) Run() error {

	t := time.NewTicker(b.FlushFrequency)
BUFFERERLOOP:
	for {
		select {
		case <-t.C:
			if len(b.Input) > 0 {
				if err := b.Flush(); err != nil {
					return err
				}
			}

		case <-b.Shutdown:
			log.Println("Shutting down")
			b.Flush()
			b.SaveIndex()
			break BUFFERERLOOP
		}
	}
	return nil
}

// Stats collects statistics from the Bufferer
func (b *Bufferer) Stats() ([]models.Point, error) {

	var pts []models.Point
	// TODO: add individual db stats
	b.Lock.Lock()
	defer b.Lock.Unlock()
	tags := models.NewTags(map[string]string{})

	var s int
	for _, bf := range b.Index {
		s += bf.NumMetrics
	}
	fields := map[string]interface{}{
		"files":       len(b.Index),
		"num_metrics": s,
	}

	pt, _ := models.NewPoint("sir_relaybuffer", tags, fields, time.Now())
	pts = append(pts, pt)
	return pts, nil
}
