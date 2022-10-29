package main

import (
	"errors"
	"fmt"
	"github.com/yusaint/gostream/arrays"
	"github.com/yusaint/gostream/gostream-contrib/filestream"
	"github.com/yusaint/gostream/gostream-contrib/mq"
	"github.com/yusaint/gostream/stream"
	"github.com/yusaint/gostream/stream/functions"
	"github.com/yusaint/gostream/stream/ops"
	"os"
	"time"
)

func main() {
	dd()
}

var IntLte = ops.NewSort(functions.IntLte)

func bb() {
	stream := stream.Stream[int](arrays.Of(1, 2, 3))
	sum, err := stream.
		Filter(ops.Filter(func(t int) (bool, error) { return t > -1, errors.New("aaaaa") })).
		Distinct().
		Skip(0).Limit(30).
		Sort(IntLte).
		Reduce(ops.IntSum)
	if err != nil {
		fmt.Println("!!!", err.Error(), sum)
	} else {
		fmt.Println(sum)
	}
}

type Record struct {
	R1 string
	R2 string
	R3 string
}

func cc() {
	f, err := os.Open("./demo.csv")
	if err != nil {
		panic(err)
	}
	stream.Stream[[]string](filestream.NewCsvFileStream(f)).Map(ops.Map(func(row []string) (*Record, error) {
		return &Record{
			R1: row[0],
			R2: row[1],
			R3: row[2],
		}, nil
	})).Filter(ops.Filter(func(r *Record) (bool, error) {
		return r.R1 == "3", nil
	})).Foreach(ops.Print)
}

type XX struct {
	ts int64
}

func dd() {
	channelStream := mq.NewChannelStream[int](10)
	go func() {
		counter := 0
		for {
			channelStream.Send(counter)
			counter++
			if counter%100 == 0 {
				time.Sleep(1 * time.Second)
			}
		}
	}()
	stream.Stream[int](channelStream).Parallel(ops.WithFixedPool(20)).Foreach(ops.Foreach[int](func(e int) error {
		fmt.Println(e)
		return nil
	}))

}
