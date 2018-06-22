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
)

var bufferSizeMax = 10000

type BufferFile struct {
	Filename        string `json:"filename"`
	NumMetrics      int    `json:"num_metrics"`
	Database        string `json:"database"`
	RetentionPolicy string `json:"retention_policy"`
	Precision       string `json:"precision"`
}

type Bufferer struct {
	Input          chan client.BatchPoints
	Output         chan client.BatchPoints
	Index          []*BufferFile
	RootPath       string
	FlushFrequency time.Duration
	Shutdown       chan struct{}
	Lock           sync.Mutex
}

type BatchBuffer struct {
	Database        string `json:"database"`
	RetentionPolicy string `json:"retention_policy"`
	Precision       string `json:"precision"`
	Points          string `json:"points"`
}

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

func NewBufferFile() *BufferFile {
	return &BufferFile{}
}

func NewBufferFileFromBP(bp client.BatchPoints) *BufferFile {

	bf := BufferFile{
		Database:        bp.Database(),
		RetentionPolicy: bp.RetentionPolicy(),
		Precision:       bp.Precision(),
		NumMetrics:      len(bp.Points()),
		Filename:        fmt.Sprintf("%d-%s-%s-%s.json", time.Now().UnixNano(), bp.Database(), bp.RetentionPolicy(), bp.Precision()),
	}

	return &bf
}

func (bf *BufferFile) String() string {
	return fmt.Sprintf("%s-%s-%s", bf.Database, bf.RetentionPolicy, bf.Precision)
}

func NewBufferer() *Bufferer {
	b := Bufferer{}
	b.Index = make([]*BufferFile, 0)
	b.RootPath, _ = os.Getwd()
	b.Input = make(chan client.BatchPoints, bufferSizeMax)
	b.FlushFrequency = 5 * time.Minute
	b.Shutdown = make(chan struct{})
	return &b
}

type BufferSave struct {
	Index []*BufferFile `json:"bufferfiles"`
}

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
	return nil

}

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

	if err = json.Unmarshal(bf, &bb); err != nil {
		return nil, err
	}

	bp, err := bb.BatchPoints()
	if err != nil {
		return nil, err
	}

	b.Index = b.Index[1:]
	return bp, nil

}

func (b *Bufferer) Run() error {

	t := time.NewTicker(b.FlushFrequency)
BUFFERERLOOP:
	for {
		select {
		case <-t.C:
			if len(b.Input) > 0 {
				log.Println("Flushing down")
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

// func (b *Bufferer) Archive(bp client.BatchPoints) error {
// 	// add file

// 	// add to index
// }

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

// func (b *Bufferer) Flush() error {

// }s

// func (b *Bufferer) Stop() error {

// }
