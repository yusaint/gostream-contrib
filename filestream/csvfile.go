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

type CsvFileStreamCfg struct {
	DownloadTimeout int64
}

type CsvFileStreamOption func(cfg *CsvFileStreamCfg)

func WithDownloadTimeout(timeout int64) CsvFileStreamOption {
	return func(cfg *CsvFileStreamCfg) {
		cfg.DownloadTimeout = timeout
	}
}

func NewCsvFileStreamByUrl(url string, options ...CsvFileStreamOption) (*CsvFileStream, error) {
	defaultCfg := CsvFileStreamCfg{
		DownloadTimeout: 5,
	}
	for _, opt := range options {
		opt(&defaultCfg)
	}
	client := &http.Client{
		Timeout: time.Duration(defaultCfg.DownloadTimeout) * time.Second,
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
		if err == io.EOF {
			return false, nil
		}
		return false, err
	} else {
		return true, sink.Accept(record)
	}
}
