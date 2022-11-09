package main

import (
	"fmt"
	"time"

	"github.com/yusaint/gostream/arrays"
	"github.com/yusaint/gostream/gostream-contrib/mq"
	"github.com/yusaint/gostream/stream"
	"github.com/yusaint/gostream/stream/functions"
	"github.com/yusaint/gostream/stream/ops"
)

func main() {
	bb()
}

var IntLte = ops.NewSort(functions.IntLte)

type User struct {
	Country string
	Name    string
}

var elems = []*User{
	{
		Country: "China",
		Name:    "1",
	},
	{
		Country: "China",
		Name:    "2",
	},
	{
		Country: "USA",
		Name:    "3",
	},
}

func bb() {
	stream := stream.Stream[*User](arrays.Of(elems...))

	var reduceFunc = func(output []string, e1 []*User) ([]string, error) {
		for _, v := range e1 {
			output = append(output, v.Name)
		}
		return output, nil
	}
	_ = reduceFunc

	var _ = func(m map[string][]*User) ([]*User, error) {
		result := make([]*User, 0)
		for _, values := range m {
			result = append(result, values...)
		}
		return result, nil
	}
	stream.
		Parallel().
		Group(ops.NewGroup(func(u *User) string {
			return u.Country
		})).Foreach(ops.PrintJson)
	//Reduce(ops.NewReduce([]string{}, reduceFunc))
	//fmt.Println(result, err)
}

type Record struct {
	R1 string
	R2 string
	R3 string
}

func cc() {

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
