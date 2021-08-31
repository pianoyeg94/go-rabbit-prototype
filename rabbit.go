package rabbit

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type ConfigParser func(config *Config) error

func New(parser ConfigParser) (*Rabbit, error) {
	var config Config
	if err := parser(&config); err != nil {
		return nil, errors.Wrap(err, "go-rabbit")
	}

	rabbit := Rabbit{
		config:         &config,
		publishers:     newPublisherRegistry(),
		consumers:      newConsumerRegistry(),
		errorSignaling: newErrorSignaling(),
		orchestration:  newOrchestration(len(config.Publishers), len(config.Consumers)),
	}

	if err := rabbit.setUpPublishers(); err != nil {
		return nil, rabbit.wrapError(err)
	}

	if err := rabbit.setUpConsumers(); err != nil {
		return nil, rabbit.wrapError(err)
	}

	return &rabbit, nil
}

type Rabbit struct {
	config                   *Config
	publisherConnection      *amqp.Connection
	consumerConnection       *amqp.Connection
	publishers               *publisherRegistry
	consumers                *consumerRegistry
	publisherConfirmsFactory NewPublisherConfirms
	errorSignaling
	orchestration
}

func (r *Rabbit) PublishAndConsume() error {
	if err := r.connectPublishers(); err != nil {
		return err
	}

	if err := r.connectConsumers(); err != nil {
		return err
	}

	if r.publisherConfirmsFactory != nil {
		r.publishers.forEach(func(publisher *publisher) {
			publisher.setOutstandingConfirms(r.publisherConfirmsFactory())
		})
	}

	r.initShutdown()

	r.publishers.forEachConcurrent(func(p *publisher) {
		p.startPubLoop()
	})

	r.consumers.forEachConcurrent(func(c *consumer) {
		c.startConsuming()
	})

	return <-r.fatalErrors
}

func (r *Rabbit) Publisher(name string) (*publisher, bool) {
	return r.publishers.get(name)
}

func (r *Rabbit) SetConsumerHandler(name string, handler ConsumerHandler) error {
	consumer, ok := r.consumers.get(name)
	if !ok {
		return r.wrapErrorMsg(fmt.Sprintf("consumer %s does not exist", name))
	}

	consumer.setHandler(handler)

	return nil
}

func (r *Rabbit) Errors() <-chan error {
	return r.errorSink
}

func (r *Rabbit) SetPublisherConfirmsFactory(factory NewPublisherConfirms) {
	r.publisherConfirmsFactory = factory
}

func (r *Rabbit) Shutdown(ctx context.Context) error {
	select {
	case <-r.stop:
		return r.wrapErrorMsg("rabbit app already closed")
	default:
		close(r.stop)
	}

	select {
	case <-r.close:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.publisherConnection.Close()
	r.consumerConnection.Close()

	return nil
}

func (r *Rabbit) Close() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}

	r.publisherConnection.Close()
	r.consumerConnection.Close()
}

func (r *Rabbit) setUpPublishers() error {
	for i, config := range r.config.Publishers {
		if err := r.validatePublisher(config, i); err != nil {
			return err
		}

		exchange, ok := r.config.getExchange(config.Exchange)
		if !ok {
			return errors.New(fmt.Sprintf("exchange config for publisher '%s' not found", config.Name))
		}

		publisher := newPublisher(
			config,
			r.config.ChannelReconnect,
			newTopologer(exchange, nil),
			r.errorSignaling,
			r.orchestration,
		)

		r.publishers.set(publisher.name, publisher)
	}

	return nil
}

func (r *Rabbit) connectPublishers() error {
	var err error
	r.publisherConnection, err = amqp.DialConfig(r.config.DSN, amqp.Config{
		Heartbeat:  time.Duration(r.config.Connection.Heartbeat) * time.Second,
		ChannelMax: r.config.Connection.MaxChannels,
		Dial:       amqp.DefaultDial(time.Duration(r.config.Connection.Timeout) * time.Second),
		Properties: r.config.Connection.Properties,
	})
	if err != nil {
		return err
	}

	r.monitorErrors(r.publisherConnection, r.recoverPublishers)
	r.monitorFlowControl()

	r.publishers.forEach(func(p *publisher) {
		p.setConnection(r.publisherConnection)
	})

	return nil
}

func (r *Rabbit) recoverPublishers() {
	r.recoverFromError(r.connectPublishers, r.pausePublishers, "publisher")
}

func (r *Rabbit) validatePublisher(config *PublisherConfig, idx int) error {
	if config.Name == "" {
		return errors.New(fmt.Sprintf("publisher at idx %v without a name", idx))
	}

	if _, ok := r.publishers.get(config.Name); ok {
		return errors.New(fmt.Sprintf("publisher with name '%s' already exists", config.Name))
	}

	if config.Exchange == "" && config.RoutingKey == "" {
		return errors.New(fmt.Sprintf("no exchange or routing key specified for publisher '%s'", config.Name))
	}

	return nil
}

func (r *Rabbit) setUpConsumers() error {
	for i, config := range r.config.Consumers {
		if err := r.validateConsumer(config, i); err != nil {
			return err
		}

		queue, ok := r.config.getQueue(config.Queue)
		if !ok {
			return errors.New(fmt.Sprintf("exchange config for consumer '%s' not found", config.Name))
		}

		if queue.Binding.Exchange == "" {
			return errors.New(fmt.Sprintf("consumer '%s' has no exchange to bind to", config.Name))
		}

		exchange, ok := r.config.getExchange(queue.Binding.Exchange)
		if !ok {
			return errors.New(fmt.Sprintf("exchange config for consumer '%s' not found", config.Name))
		}

		consumer := newConsumer(
			config,
			r.config.ChannelReconnect,
			newTopologer(exchange, queue),
			r.errorSignaling,
			r.orchestration,
		)

		r.consumers.set(consumer.name, consumer)
	}

	return nil
}

func (r *Rabbit) connectConsumers() error {
	var err error
	r.consumerConnection, err = amqp.DialConfig(r.config.DSN, amqp.Config{
		Heartbeat:  time.Duration(r.config.Connection.Heartbeat) * time.Second,
		ChannelMax: r.config.Connection.MaxChannels,
		Dial:       amqp.DefaultDial(time.Duration(r.config.Connection.Timeout) * time.Second),
		Properties: r.config.Connection.Properties,
	})
	if err != nil {
		return err
	}

	r.monitorErrors(r.consumerConnection, r.recoverConsumers)

	r.consumers.forEach(func(c *consumer) {
		c.setConnection(r.consumerConnection)
	})

	return nil
}

func (r *Rabbit) recoverConsumers() {
	r.recoverFromError(r.connectConsumers, r.pauseConsumers, "consumer")
}

func (r *Rabbit) validateConsumer(config *ConsumerConfig, idx int) error {
	if config.Name == "" {
		return errors.New(fmt.Sprintf("consumer at idx %v without a name", idx))
	}

	if _, ok := r.consumers.get(config.Name); ok {
		return errors.New(fmt.Sprintf("consumer with name '%s' already exists", config.Name))
	}

	return nil
}

func (r *Rabbit) recoverFromError(connect func() error, pauseFunc func(chan struct{}), connType string) {
	resume := make(chan struct{})
	pauseFunc(resume)

	go func() {
		if r.config.ConnectionReconnect.MaxRetries < 0 {
			r.signalFatalError(r.wrapErrorMsg(fmt.Sprintf("%s connection errored out", connType)))
			return
		}

		var retries int
		curr := r.config.ConnectionReconnect.IntervalStart
		stop := r.config.ConnectionReconnect.IntervalMax + r.config.ConnectionReconnect.IntervalStart

		for {
			select {
			case <-r.stop:
				return
			default:
				if r.config.ConnectionReconnect.MaxRetries != 0 && retries >= r.config.ConnectionReconnect.MaxRetries {
					r.signalFatalError(r.wrapErrorMsg(fmt.Sprintf("max %s connection recovery retries exceeded", connType)))
					return
				}
				time.Sleep(time.Duration(curr) * time.Millisecond)
				if err := connect(); err != nil {
					retries++
					if r.config.ConnectionReconnect.IntervalMax == 0 || curr <= stop {
						curr += r.config.ConnectionReconnect.IntervalStep
					}
					r.signalError(r.wrapError(err))
					continue
				}
				close(resume)
				return
			}
		}
	}()
}

func (r *Rabbit) monitorErrors(connection *amqp.Connection, recover func()) {
	go func() {
		errors := connection.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case err := <-errors:
			if err != nil {
				r.signalError(r.wrapError(err))
				recover()
			}
			return
		case <-r.stop:
			return
		}
	}()
}

func (r *Rabbit) monitorFlowControl() {
	go func() {
		flowControl := r.publisherConnection.NotifyBlocked(make(chan amqp.Blocking))
		for blocking := range flowControl {
			if blocking.Active {
				resume := make(chan struct{})
				r.pauseOnFlowControl(resume)
				<-flowControl
				close(resume)
			}
		}
	}()
}

func (r *Rabbit) wrapErrorMsg(msg string) error {
	return r.wrapError(errors.New(msg))
}

func (r *Rabbit) wrapError(err error) error {
	return errors.Wrap(err, "go-rabbit")
}
