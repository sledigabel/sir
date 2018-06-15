package endpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	Input    chan client.BatchPoints
	Output   chan client.BatchPoints
	Index    []*BufferFile
	RootPath string
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

func NewBufferer() *Bufferer {
	var b Bufferer
	b.Index = make([]*BufferFile, 0)
	b.RootPath, _ = os.Getwd()
	b.Input = make(chan client.BatchPoints, bufferSizeMax)
	return &b
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

	return err

}

func (b *Bufferer) Write(bp client.BatchPoints) error {

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

// func (b *Bufferer) Archive(bp client.BatchPoints) error {
// 	// add file

// 	// add to index
// }

// func (b *Bufferer) Stats() error {

// }

// func (b *Bufferer) Flush() error {

// }

// func (b *Bufferer) Stop() error {

// }
