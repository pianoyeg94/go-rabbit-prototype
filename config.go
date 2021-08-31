package rabbit

import (
	"github.com/streadway/amqp"
)

type Config struct {
	DSN                 string             `json:"dsn"`
	Connection          ConnectionConfig   `json:"connection"`
	ConnectionReconnect ReconnectConfig    `json:"connection_reconnect"`
	ChannelReconnect    ReconnectConfig    `json:"channel_reconnect"`
	Exchanges           []*ExchangeConfig  `json:"exchanges"`
	Queues              []*QueueConfig     `json:"queues"`
	Publishers          []*PublisherConfig `json:"publishers"`
	Consumers           []*ConsumerConfig  `json:"consumers"`
}

func (c *Config) getExchange(name string) (*ExchangeConfig, bool) {
	for _, e := range c.Exchanges {
		if e.Name == name {
			return e, true
		}
	}

	return nil, false
}

func (c *Config) getQueue(name string) (*QueueConfig, bool) {
	for _, q := range c.Queues {
		if q.Name == name {
			return q, true
		}
	}

	return nil, false
}

type ConnectionConfig struct {
	Timeout     int64      `json:"timeout"`
	Heartbeat   int64      `json:"heartbeat"`
	MaxChannels int        `json:"max_channels"`
	Properties  amqp.Table `json:"properties"`
}

type ReconnectConfig struct {
	IntervalStart uint64 `json:"interval_start"`
	IntervalStep  uint64 `json:"interval_step"`
	IntervalMax   uint64 `json:"interval_max"`
	MaxRetries    int    `json:"max_retries"`
}

type PublisherConfig struct {
	Name              string `json:"name"`
	Exchange          string `json:"exchange"`
	RoutingKey        string `json:"routing_key"`
	ReplyTo           string `json:"reply_to"`
	InitialBufferSize int    `json:"buffer_size"`
	ConfirmMode       bool   `json:"confirm_mode"`
	ConfirmExpiration int64  `json:"confirm_expiration"`
	MaxRepublish      int    `json:"max_republish"`
	PersisentMode     bool   `json:"persistent_mode"`
	ContentType       string `json:"content_type"`
	ContentEncoding   string `json:"content_encoding"`
	MessageType       string `json:"message_type"`
	MessageTTL        string `json:"message_ttl"`
}

type ConsumerConfig struct {
	Name               string     `json:"name"`
	Queue              string     `json:"queue"`
	Concurrency        int        `json:"concurrency"`
	PrefetchMultiplier int        `json:"prefetch_multiplier"`
	Exclusive          bool       `json:"exclusive"`
	AutoAck            bool       `json:"auto_ack"`
	Options            amqp.Table `json:"options"`
}

type ExchangeConfig struct {
	Name       string     `json:"name"`
	Type       string     `json:"type"`
	Durable    bool       `json:"durable"`
	AutoDelete bool       `json:"auto_delete"`
	Options    amqp.Table `json:"options"`
}

type QueueConfig struct {
	Name       string        `json:"name"`
	Binding    BindingConfig `json:"binding"`
	Durable    bool          `json:"durable"`
	AutoDelete bool          `json:"auto_delete"`
	Exclusive  bool          `json:"exclusive"`
	Options    amqp.Table    `json:"options"`
}

type BindingConfig struct {
	Exchange string     `json:"exchange"`
	Key      string     `json:"key"`
	Options  amqp.Table `json:"options"`
}
