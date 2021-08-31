package rabbit

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const (
	defaultInitialBuffSize   = 10
	defaultConfirmExpiration = 10 // seconds
	defaultMaxRepublish      = 5
)

const (
	loopInactive int32 = iota
	loopActive
)

func newMessage(headers amqp.Table, body []byte, corrId string) Message {
	return Message{
		Headers:       headers,
		Body:          body,
		CorrelationId: corrId,
	}
}

type Message struct {
	Headers       amqp.Table
	Body          []byte
	CorrelationId string
	NumPublished  int
}

func newPublisher(
	config *PublisherConfig,
	reconnect ReconnectConfig,
	topologer *topologer,
	errSig errorSignaling,
	orch orchestration,
) *publisher {
	initialBuffSize := config.InitialBufferSize
	if initialBuffSize <= 0 {
		initialBuffSize = defaultInitialBuffSize
	}

	maxRepublish := config.MaxRepublish
	if maxRepublish <= 0 {
		maxRepublish = defaultMaxRepublish
	}

	confirmExpiration := config.ConfirmExpiration
	if config.ConfirmMode && confirmExpiration <= 0 {
		confirmExpiration = defaultConfirmExpiration
	}

	return &publisher{
		topologer:         topologer,
		reconnect:         reconnect,
		messageQueue:      queue.New(int64(initialBuffSize)),
		messages:          make(chan Message),
		pause:             orch.publisherPause,
		flowControl:       orch.flowControl,
		stop:              orch.stop,
		errorSignaling:    errSig,
		name:              config.Name,
		exchange:          config.Exchange,
		routingKey:        config.RoutingKey,
		replyTo:           config.ReplyTo,
		initialBuffSize:   initialBuffSize,
		confirmMode:       config.ConfirmMode,
		confirmExpiration: confirmExpiration,
		maxRepublish:      maxRepublish,
		persistentMode:    config.PersisentMode,
		contentType:       config.ContentType,
		contentEncoding:   config.ContentEncoding,
		messageType:       config.MessageType,
		messageTTL:        config.MessageTTL,
	}
}

type publisher struct {
	connection   *amqp.Connection
	channel      *amqp.Channel
	connMx       sync.Mutex
	channelMx    sync.Mutex
	reconnect    ReconnectConfig
	topologer    *topologer
	messageQueue *queue.Queue
	messages     chan Message

	once                sync.Once
	loopStatus          int32
	deliveryTag         uint64
	flowControl         chan chan struct{}
	pause               chan chan struct{}
	stop                chan struct{}
	shutdown            chan chan struct{}
	outstandingConfirms PublisherConfirms
	errorSignaling

	name              string
	exchange          string
	routingKey        string
	replyTo           string
	initialBuffSize   int
	confirmMode       bool
	confirmExpiration int64
	maxRepublish      int
	persistentMode    bool
	contentType       string
	contentEncoding   string
	messageType       string
	messageTTL        string
}

func (p *publisher) Publish(msg []byte, corrId string, headers amqp.Table) error {
	if err := p.messageQueue.Put(newMessage(headers, msg, corrId)); err != nil {
		return p.wrapErrorMsg("already stopped")
	}

	return nil
}

func (p *publisher) startPubLoop() error {
	if err := p.connect(); err != nil {
		return p.wrapError(err)
	}

	if err := p.declareExchange(); err != nil {
		return p.wrapError(err)
	}

	if p.confirmMode {
		p.resetDeliveryTag()
		p.monitorConfirms()
	}

	p.oneTimeSetup()

	if p.markLoopActive() {
		p.spawnWorker()
	}

	p.monitorErrors()

	return nil
}

func (p *publisher) connect() error {
	p.channelMx.Lock()
	defer p.channelMx.Unlock()

	var err error
	oldChannel := p.channel

	p.connMx.Lock()
	p.channel, err = p.connection.Channel()
	p.connMx.Unlock()
	if err != nil {
		p.channel = oldChannel
		return err
	}

	if p.confirmMode {
		if err := p.channel.Confirm(false); err != nil {
			p.channel.Close()
			return err
		}
	}

	return nil
}

func (p *publisher) declareExchange() error {
	p.topologer.setChannel(p.channel)
	if err := p.topologer.declareExchange(); err != nil {
		p.channel.Close()
		return err
	}

	return nil
}

func (p *publisher) oneTimeSetup() {
	p.once.Do(func() {
		p.proxyMessagesToWorker()
		p.cleanUpOnShutdown()
		if p.confirmMode {
			if p.outstandingConfirms == nil {
				p.outstandingConfirms = newSimplePublisherConfirms()
			}
			p.republishExpiredConfirms()
		}
	})
}

func (p *publisher) spawnWorker() {
	go func() {
		for {
			select {
			case msg := <-p.messages:
				if err := p.publish(msg); err != nil {
					p.markLoopInactive()
					p.signalError(p.wrapError(err))
					if err := p.republish(msg); err != nil {
						p.signalError(p.wrapError(err))
					}
					return
				}
			case <-p.stop:
				return
			}
		}
	}()
}

func (p *publisher) proxyMessagesToWorker() {
	go func() {
		for {
			msgs, err := p.messageQueue.Get(1)
			if err != nil {
				return
			}
			msg, ok := msgs[0].(Message)
			if ok {
				p.messages <- msg
			}
		}
	}()
}

func (p *publisher) recoverFromError() {
	go func() {
		if p.reconnect.MaxRetries < 0 {
			p.signalFatalError(p.wrapErrorMsg("channel errored out"))
			return
		}

		var retries int
		curr := p.reconnect.IntervalStart
		stop := p.reconnect.IntervalMax + p.reconnect.IntervalStart

		for {
			select {
			case <-p.stop:
				return
			case resume := <-p.pause:
				select {
				case <-resume:
				case <-p.stop:
					return
				}
			default:
				if p.reconnect.MaxRetries != 0 && retries >= p.reconnect.MaxRetries {
					p.signalFatalError(p.wrapErrorMsg("max channel recovery retries exceeded"))
					return
				}
				time.Sleep(time.Duration(curr) * time.Millisecond)
				if err := p.startPubLoop(); err != nil {
					retries++
					if p.reconnect.IntervalMax == 0 || curr <= stop {
						curr += p.reconnect.IntervalStep
					}
					p.signalError(err)
					continue
				}
				return
			}
		}
	}()
}

func (p *publisher) monitorErrors() {
	go func() {
		errors := p.channel.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case err := <-errors:
			if err != nil {
				p.signalError(p.wrapError(err))
				p.recoverFromError()
			}
			return
		case <-p.stop:
			return
		}
	}()
}

func (p *publisher) monitorConfirms() {
	acks, nacks := p.channel.NotifyConfirm(
		make(chan uint64, p.initialBuffSize),
		make(chan uint64, p.initialBuffSize/3),
	)

	go func() {
		for {
			select {
			case tag, ok := <-acks:
				if !ok {
					return
				}
				p.outstandingConfirms.Pop(tag)
			case <-p.stop:
				for tag := range acks {
					_ = tag
				}
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case tag, ok := <-nacks:
				if !ok {
					return
				}
				confirm, ok := p.outstandingConfirms.Pop(tag)
				if ok {
					if err := p.republish(confirm.Message); err != nil {
						p.signalError(p.wrapError(err))
					}
				}
			case <-p.stop:
				for tag := range nacks {
					_ = tag
				}
				return
			}
		}
	}()
}

func (p *publisher) republishExpiredConfirms() {
	go func() {
		ticker := time.NewTicker(time.Duration(p.confirmExpiration/2) * time.Second)
		for {
			select {
			case <-ticker.C:
				for expired := range p.outstandingConfirms.Expired() {
					confirm, ok := p.outstandingConfirms.Pop(expired)
					if ok {
						if err := p.republish(confirm.Message); err != nil {
							p.signalError(p.wrapError(err))
						}
					}
				}
			case <-p.stop:
				ticker.Stop()
				return
			}
		}
	}()
}

func (p *publisher) publish(msg Message) error {
	deliveryMode := amqp.Transient
	if p.persistentMode {
		deliveryMode = amqp.Persistent
	}

	select {
	case resume := <-p.flowControl:
		select {
		case <-resume:
		case <-p.stop:
			return errors.New("already stopped")
		}
	default:
	}

	p.channelMx.Lock()
	err := p.channel.Publish(
		p.exchange,
		p.routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         msg.Headers,
			ContentType:     p.contentType,
			ContentEncoding: p.contentEncoding,
			DeliveryMode:    deliveryMode,
			CorrelationId:   msg.CorrelationId,
			ReplyTo:         p.replyTo,
			Expiration:      p.messageTTL,
			Timestamp:       time.Now().UTC(),
			Type:            p.messageType,
			Body:            msg.Body,
		},
	)
	p.channelMx.Unlock()
	if err != nil {
		return err
	}

	var isStopped bool
	select {
	case <-p.stop:
		isStopped = true
	default:
	}

	if p.confirmMode && !isStopped {
		confirmExpiration := p.confirmExpiration
		if confirmExpiration <= 0 {
			confirmExpiration = defaultConfirmExpiration
		}
		msg.NumPublished++
		p.outstandingConfirms.Set(
			p.incrementDeliveryTag(),
			newConfirmItem(msg, confirmExpiration),
		)
	}

	return nil
}

func (p *publisher) republish(msg Message) error {
	if msg.NumPublished >= p.maxRepublish {
		return errors.New("message dropped, max republish exceeded")
	}

	if err := p.messageQueue.Put(msg); err != nil {
		return errors.New("already stopped")
	}

	return nil
}

func (p *publisher) cleanUpOnShutdown() {
	go func() {
		done := make(chan struct{})
		p.shutdown <- done
		<-p.stop

		unpublished := p.messageQueue.Dispose()
		select {
		case msg := <-p.messages:
			unpublished = append(unpublished, msg)
		default:
		}

		for _, msg := range unpublished {
			m, ok := msg.(Message)
			if ok {
				if err := p.publish(m); err != nil {
					break
				}
			}
		}

		p.channelMx.Lock()
		p.channel.Close()
		p.channelMx.Unlock()

		close(done)
	}()
}

func (p *publisher) setConnection(conn *amqp.Connection) {
	p.connMx.Lock()
	defer p.connMx.Unlock()
	p.connection = conn
}

func (p *publisher) setOutstandingConfirms(confirms PublisherConfirms) {
	p.outstandingConfirms = confirms
}

func (p *publisher) markLoopActive() bool {
	return atomic.CompareAndSwapInt32(&p.loopStatus, loopInactive, loopActive)
}

func (p *publisher) markLoopInactive() {
	atomic.StoreInt32(&p.loopStatus, loopInactive)
}

func (p *publisher) incrementDeliveryTag() uint64 {
	return atomic.AddUint64(&p.deliveryTag, 1)
}

func (p *publisher) resetDeliveryTag() {
	atomic.StoreUint64(&p.deliveryTag, 0)
}

func (p *publisher) wrapErrorMsg(msg string) error {
	return p.wrapError(errors.New(msg))
}

func (p *publisher) wrapError(err error) error {
	return errors.Wrapf(err, "publisher: %s", p.name)
}
