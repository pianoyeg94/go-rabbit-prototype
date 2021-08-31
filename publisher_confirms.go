package rabbit

import (
	"sync"
	"time"
)

type NewPublisherConfirms func() PublisherConfirms

type PublisherConfirms interface {
	Set(deliveryTag uint64, item ConfirmItem)
	Pop(deliveryTag uint64) (ConfirmItem, bool)
	Expired() <-chan uint64
}

func newConfirmItem(msg Message, expiresIn int64) ConfirmItem {
	return ConfirmItem{
		Message: msg,
		Expires: time.Now().Unix() + expiresIn,
	}
}

type ConfirmItem struct {
	Message Message
	Expires int64
}

func newSimplePublisherConfirms() *simplePublisherConfirms {
	return &simplePublisherConfirms{
		outstandingConfirms: make(map[uint64]ConfirmItem),
	}
}

type simplePublisherConfirms struct {
	sync.Mutex
	outstandingConfirms map[uint64]ConfirmItem
}

func (s *simplePublisherConfirms) Set(deliveryTag uint64, item ConfirmItem) {
	s.Lock()
	defer s.Unlock()
	s.outstandingConfirms[deliveryTag] = item
}

func (s *simplePublisherConfirms) Pop(deliveryTag uint64) (ConfirmItem, bool) {
	s.Lock()
	defer s.Unlock()
	item, ok := s.outstandingConfirms[deliveryTag]
	if !ok {
		return ConfirmItem{}, false
	}
	delete(s.outstandingConfirms, deliveryTag)
	return item, true
}

func (s *simplePublisherConfirms) Expired() <-chan uint64 {
	var expired []uint64
	s.Lock()
	for k, v := range s.outstandingConfirms {
		if time.Now().Unix() > v.Expires {
			expired = append(expired, k)
		}
	}
	s.Unlock()

	expiredChan := make(chan uint64)
	go func() {
		defer close(expiredChan)
		for _, e := range expired {
			expiredChan <- e
		}
	}()

	return expiredChan
}
