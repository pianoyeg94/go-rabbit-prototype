package rabbit

import "sync"

func newConsumerRegistry() *consumerRegistry {
	return &consumerRegistry{
		consumers: make(map[string]*consumer),
	}
}

type consumerRegistry struct {
	sync.Mutex
	consumers map[string]*consumer
}

func (cr *consumerRegistry) set(name string, consumer *consumer) {
	cr.Lock()
	defer cr.Unlock()
	cr.consumers[name] = consumer
}

func (cr *consumerRegistry) get(name string) (*consumer, bool) {
	cr.Lock()
	defer cr.Unlock()
	consumer, ok := cr.consumers[name]
	return consumer, ok
}

func (cr *consumerRegistry) forEach(fn func(*consumer)) {
	cr.Lock()
	defer cr.Unlock()
	for _, c := range cr.consumers {
		fn(c)
	}
}

func (cr *consumerRegistry) forEachConcurrent(fn func(*consumer)) {
	cr.Lock()
	defer cr.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(cr.consumers))

	for _, c := range cr.consumers {
		go func(consumer *consumer) {
			defer wg.Done()
			fn(consumer)
		}(c)
	}

	wg.Wait()
}
