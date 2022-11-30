package filestream

import (
	"bytes"
	"encoding/csv"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/yusaint/gostream/generic"
)

type CsvFileStream struct {
	file   *os.File
	reader *csv.Reader
}

func NewCsvFileStreamByFile(file *os.File) *CsvFileStream {
	return &CsvFileStream{
		file:   file,
		reader: csv.NewReader(file),
	}
}

func NewCsvFileStreamByUrl(url string) (*CsvFileStream, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &CsvFileStream{
		reader: csv.NewReader(bytes.NewReader(buf)),
	}, nil
}

func (c *CsvFileStream) EstimatedSize() int64 {
	return -1
}

func (c *CsvFileStream) ForeachRemaining(sink generic.Consumer) error {
	for {
		isContinue, err := c.TryAdvance(sink)
		if err != nil {
			return err
		} else {
			if !isContinue {
				return nil
			}
		}
	}
}

func (c *CsvFileStream) TryAdvance(sink generic.Consumer) (bool, error) {
	if record, err := c.reader.Read(); err != nil {
		return false, err
	} else {
		return true, sink.Accept(record)
	}
}
