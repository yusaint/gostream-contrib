package filestream

import (
	"encoding/csv"
	"github.com/yusaint/gostream/generic"
	"os"
)

type CsvFileStream struct {
	file   *os.File
	reader *csv.Reader
}

func NewCsvFileStream(file *os.File) *CsvFileStream {
	return &CsvFileStream{
		file:   file,
		reader: csv.NewReader(file),
	}
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
