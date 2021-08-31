package rabbit

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type ConsumerHandler func(delivery amqp.Delivery) error

func newConsumer(
	config *ConsumerConfig,
	reconnect ReconnectConfig,
	topologer *topologer,
	errSig errorSignaling,
	orch orchestration,
) *consumer {
	concurrency := config.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	prefetchMultiplier := config.PrefetchMultiplier
	if prefetchMultiplier <= 0 {
		prefetchMultiplier = 1
	}

	return &consumer{
		topologer:          topologer,
		reconnect:          reconnect,
		pause:              orch.consumerPause,
		stop:               orch.stop,
		shutdown:           orch.shutdown,
		errorSignaling:     errSig,
		name:               config.Name,
		queue:              config.Queue,
		concurrency:        concurrency,
		prefetchMultiplier: prefetchMultiplier,
		exclusive:          config.Exclusive,
		autoAck:            config.AutoAck,
		options:            config.Options,
	}
}

type consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	connMx     sync.Mutex
	reconnect  ReconnectConfig
	handler    ConsumerHandler
	topologer  *topologer

	once     sync.Once
	pause    chan chan struct{}
	stop     chan struct{}
	shutdown chan chan struct{}
	errorSignaling

	name               string
	queue              string
	concurrency        int
	prefetchMultiplier int
	exclusive          bool
	autoAck            bool
	options            amqp.Table
}

func (c *consumer) startConsuming() error {
	if err := c.connect(); err != nil {
		return c.wrapError(err)
	}

	if err := c.declareQueue(); err != nil {
		return c.wrapError(err)
	}

	deliveries, err := c.channel.Consume(
		c.queue,
		c.name,
		c.autoAck,
		c.exclusive,
		false, // noLocal
		false, // noWait
		c.options,
	)
	if err != nil {
		c.channel.Close()
		return c.wrapError(err)
	}

	c.oneTimeSetup()

	for i := 0; i < c.concurrency; i++ {
		c.spawnWorker(deliveries)
	}

	c.monitorErrors()

	return nil
}

func (c *consumer) connect() error {
	var err error
	oldChannel := c.channel

	c.connMx.Lock()
	c.channel, err = c.connection.Channel()
	c.connMx.Unlock()
	if err != nil {
		c.channel = oldChannel
		return err
	}

	if err := c.channel.Qos(c.concurrency*c.prefetchMultiplier, 0, false); err != nil {
		c.channel.Close()
		return err
	}

	return nil
}

func (c *consumer) declareQueue() error {
	c.topologer.setChannel(c.channel)
	if err := c.topologer.declareExchange(); err != nil {
		c.channel.Close()
		return err
	}

	if err := c.topologer.declareQueue(); err != nil {
		c.channel.Close()
		return err
	}

	if err := c.topologer.bindQueue(); err != nil {
		c.channel.Close()
		return err
	}

	return nil
}

func (c *consumer) oneTimeSetup() {
	c.once.Do(func() {
		c.cleanUpOnShutdown()
	})
}

func (c *consumer) spawnWorker(deliveries <-chan amqp.Delivery) {
	go func() {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			if err := c.handler(delivery); err != nil {
				c.signalError(c.wrapError(err))
			}
		case <-c.stop:
			return
		}
	}()
}

func (c *consumer) recoverFromError() {
	go func() {
		if c.reconnect.MaxRetries < 0 {
			c.signalFatalError(c.wrapErrorMsg("channel errored out"))
			return
		}

		var retries int
		curr := c.reconnect.IntervalStart
		stop := c.reconnect.IntervalMax + c.reconnect.IntervalStart

		for {
			select {
			case <-c.stop:
				return
			case resume := <-c.pause:
				select {
				case <-resume:
				case <-c.stop:
					return
				}
			default:
				if c.reconnect.MaxRetries != 0 && retries >= c.reconnect.MaxRetries {
					c.signalFatalError(c.wrapErrorMsg("max channel recovery retries exceeded"))
					return
				}
				time.Sleep(time.Duration(curr) * time.Millisecond)
				if err := c.startConsuming(); err != nil {
					retries++
					if c.reconnect.IntervalMax == 0 || curr <= stop {
						curr += c.reconnect.IntervalStep
					}
					c.signalError(err)
					continue
				}
				return
			}
		}
	}()
}

func (c *consumer) monitorErrors() {
	go func() {
		errors := c.channel.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case err := <-errors:
			if err != nil {
				c.signalError(c.wrapError(err))
				c.recoverFromError()
			}
			return
		case <-c.stop:
			return
		}
	}()
}

func (c *consumer) cleanUpOnShutdown() {
	go func() {
		done := make(chan struct{})
		c.shutdown <- done
		<-c.stop

		c.channel.Close()

		close(done)
	}()
}

func (c *consumer) setHandler(handler ConsumerHandler) {
	c.handler = handler
}

func (c *consumer) setConnection(conn *amqp.Connection) {
	c.connMx.Lock()
	defer c.connMx.Unlock()
	c.connection = conn
}

func (c *consumer) wrapErrorMsg(msg string) error {
	return c.wrapError(errors.New(msg))
}

func (c *consumer) wrapError(err error) error {
	return errors.Wrapf(err, "consumer: %s", c.name)
}
