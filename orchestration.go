package rabbit

import "sync"

func newOrchestration(numPublishers int, numConsumers int) orchestration {
	return orchestration{
		publisherPause: make(chan chan struct{}, numPublishers),
		consumerPause:  make(chan chan struct{}, numConsumers),
		flowControl:    make(chan chan struct{}, numPublishers),
		stop:           make(chan struct{}),
		shutdown:       make(chan chan struct{}),
		close:          make(chan struct{}),
	}
}

type orchestration struct {
	publisherPause chan chan struct{}
	consumerPause  chan chan struct{}
	flowControl    chan chan struct{}
	stop           chan struct{}
	shutdown       chan chan struct{}
	close          chan struct{}
}

func (o orchestration) initShutdown() {
	go func() {
		var wg sync.WaitGroup
	loop:
		for {
			select {
			case g := <-o.shutdown:
				wg.Add(1)
				go func(gd chan struct{}) {
					defer wg.Done()
					<-gd
				}(g)
			case <-o.shutdown:
				break loop
			}
		}

		wg.Wait()
		close(o.close)
	}()
}

func (o orchestration) flushPauseChan(pause chan chan struct{}) {
	for {
		select {
		case <-pause:
		default:
			return
		}
	}
}

func (o orchestration) pausePublishers(resume chan struct{}) {
	for i := 0; i < cap(o.publisherPause); i++ {
		o.publisherPause <- resume
	}

	go func() {
		select {
		case <-resume:
			o.flushPauseChan(o.publisherPause)
			return
		case <-o.stop:
			return
		}
	}()
}

func (o orchestration) pauseConsumers(resume chan struct{}) {
	for i := 0; i < cap(o.consumerPause); i++ {
		o.consumerPause <- resume
	}

	go func() {
		select {
		case <-resume:
			o.flushPauseChan(o.consumerPause)
			return
		case <-o.stop:
			return
		}
	}()
}

func (o orchestration) pauseOnFlowControl(resume chan struct{}) {
	for i := 0; i < cap(o.flowControl); i++ {
		o.flowControl <- resume
	}

	go func() {
		select {
		case <-resume:
			o.flushPauseChan(o.flowControl)
			return
		case <-o.stop:
			return
		}
	}()
}
