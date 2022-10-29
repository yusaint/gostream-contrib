package mq

import "github.com/yusaint/gostream/generic"

type ChannelStream[T any] struct {
	ch chan T
}

func (c *ChannelStream[T]) Send(message T) {
	c.ch <- message
}

func (c *ChannelStream[T]) EstimatedSize() int64 { return -1 }

func (c *ChannelStream[T]) ForeachRemaining(sink generic.Consumer) error {
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

func (c *ChannelStream[T]) TryAdvance(sink generic.Consumer) (bool, error) {
	message := <-c.ch
	return true, sink.Accept(message)
}

func NewChannelStream[T any](bufferSize int) *ChannelStream[T] {
	return &ChannelStream[T]{
		ch: make(chan T, bufferSize),
	}
}
