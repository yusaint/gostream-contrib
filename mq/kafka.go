package mq

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/yusaint/gostream/generic"
)

type KafkaStreamCfg struct {
	Version    string
	Broker     []string
	Topics     []string
	GroupName  string
	Assignor   string
	FromOldest bool
}
type KafkaStream struct {
	client      sarama.ConsumerGroup
	messageChan chan *sarama.ConsumerMessage
	stopStream  chan struct{}
}

func (k *KafkaStream) EstimatedSize() int64 {
	return -1
}

func (k *KafkaStream) ForeachRemaining(sink generic.Consumer) error {
	for {
		isContinue, err := k.TryAdvance(sink)
		if err != nil {
			return err
		} else {
			if !isContinue {
				return nil
			}
		}
	}
}

func (k *KafkaStream) TryAdvance(sink generic.Consumer) (bool, error) {
	select {
	case message := <-k.messageChan:
		return true, sink.Accept(message.Value)
	case <-k.stopStream:
		return false, nil
	}
}

func NewKafkaStream(ctx context.Context, cfg *KafkaStreamCfg) (*KafkaStream, error) {
	stream := &KafkaStream{
		messageChan: make(chan *sarama.ConsumerMessage, 1),
		stopStream:  make(chan struct{}, 1),
	}
	config := sarama.NewConfig()
	if version, err := sarama.ParseKafkaVersion(cfg.Version); err != nil {
		return nil, err
	} else {
		config.Version = version
	}
	switch cfg.Assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	default:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	}
	if cfg.FromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	if client, err := sarama.NewConsumerGroup(cfg.Broker, cfg.GroupName, config); err != nil {
		return nil, err
	} else {
		stream.client = client
	}

	if err := stream.client.Consume(ctx, cfg.Topics, &consumer{
		messageCh:  stream.messageChan,
		stopStream: stream.stopStream,
	}); err != nil {
		return nil, err
	}
	return stream, nil
}

type consumer struct {
	messageCh  chan *sarama.ConsumerMessage
	stopStream chan struct{}
}

func (c *consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			c.messageCh <- message
		case <-session.Context().Done():
			close(c.stopStream)
			return nil
		}
	}
}
