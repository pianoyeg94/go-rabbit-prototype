package rabbit

import "github.com/streadway/amqp"

func newTopologer(exchange *ExchangeConfig, queue *QueueConfig) *topologer {
	return &topologer{
		exchange: exchange,
		queue:    queue,
	}
}

type topologer struct {
	exchange *ExchangeConfig
	queue    *QueueConfig
	channel  *amqp.Channel
}

func (t *topologer) setChannel(channel *amqp.Channel) {
	t.channel = channel
}

func (t *topologer) declareExchange() error {
	if t.exchange.Name == "" {
		return nil
	}

	return t.channel.ExchangeDeclare(
		t.exchange.Name,
		t.exchange.Type,
		t.exchange.Durable,
		t.exchange.AutoDelete,
		false, // internal
		false, // noWait
		t.exchange.Options,
	)
}

func (t *topologer) declareQueue() error {
	if t.queue.Name == "" {
		return nil
	}

	if _, err := t.channel.QueueDeclare(
		t.queue.Name,
		t.queue.Durable,
		t.queue.AutoDelete,
		t.queue.Exclusive,
		false, // noWait
		t.queue.Options,
	); err != nil {
		return err
	}

	return nil
}

func (t *topologer) bindQueue() error {
	if t.queue.Name == "" || t.queue.Binding.Exchange == "" {
		return nil
	}

	return t.channel.QueueBind(
		t.queue.Name,
		t.queue.Binding.Key,
		t.queue.Binding.Exchange,
		false, // noWait
		t.queue.Binding.Options,
	)
}
