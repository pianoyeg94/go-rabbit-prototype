package rabbit

import "sync"

func newPublisherRegistry() *publisherRegistry {
	return &publisherRegistry{
		publishers: make(map[string]*publisher),
	}
}

type publisherRegistry struct {
	sync.Mutex
	publishers map[string]*publisher
}

func (pr *publisherRegistry) set(name string, publisher *publisher) {
	pr.Lock()
	defer pr.Unlock()
	pr.publishers[name] = publisher
}

func (pr *publisherRegistry) get(name string) (*publisher, bool) {
	pr.Lock()
	defer pr.Unlock()
	publisher, ok := pr.publishers[name]
	return publisher, ok
}

func (pr *publisherRegistry) forEach(fn func(*publisher)) {
	pr.Lock()
	defer pr.Unlock()
	for _, p := range pr.publishers {
		fn(p)
	}
}

func (pr *publisherRegistry) forEachConcurrent(fn func(*publisher)) {
	pr.Lock()
	defer pr.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(pr.publishers))

	for _, p := range pr.publishers {
		go func(publisher *publisher) {
			defer wg.Done()
			fn(publisher)
		}(p)
	}

	wg.Wait()
}
