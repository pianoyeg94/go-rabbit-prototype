# go-rabbit-prototype (work in progress!)

This package is a wrapper around the streadway/amqp package.

Currently provides:
  1) A declarative approach for intitializing your RabbitMQ topology, publishers and consumers from a config source;
  2) Registering your consumers in a net/http handlers style approach;
  3) Separate connections for publishers and consumers;
  4) Automatic reconnection of the underlying RabbitMQ connections and their channels;
  5) Handling RabbitMQ's TCP pushbacks by pausing all current publishers;
  6) Automatically handling publisher confirms and republishing the messages if necessary.

What's still lacking for an initial release:
  1) Support for the Transactional outbox/Transaction log tailing pattern (work in progress).
     Data sources such as PostgeSQL and MongoDB are planned to be supported in the initial release.
  2) Test coverage, comments and documentation;
  3) Some major refactorings may also be applied.


# Usage example

Suppose we have the following config file called "rabbit_config.json" residing in the same directory as our 'main.go' entrypoint:

```
{
  "dsn": "amqp://guest:guest@localhost:5672/",
  "connection": {
    "timeout": 5,
    "heartbeat": 3,
    "max_channels": 1000
  },
  "connection_reconnect": {
    "interval_start": 2,
    "interval_step": 2,
    "interval_max": 10,
    "max_retreies": 5
  },
  "channel_reconnect": {
    "interval_start": 1,
    "interval_step": 1,
    "interval_max": 5,
    "max_retreies": 5
  },
  "exchanges": [
    {
      "name": "test.direct",
      "type": "direct",
      "durable": true
    }
  ],
  "queues": [
    {
      "name": "test.queue",
      "binding": {
        "exchange": "test.direct",
        "key": "test"
      },
      "durable": true
    }
  ],
  "publishers": [
    {
      "name": "test_publisher",
      "exchange": "test.direct",
      "routing_key": "test",
      "confirm_mode": true,
      "confirm_expiration": 2,
      "max_republish": 5,
      "persistent_mode": true,
      "content_type": "application/json",
      "content_encoding": "utf-8"
    }
  ],
  "consumers": [
    {
      "name": "test_consumer",
      "queue": "test.queue",
      "concurrency": 4,
      "prefetch_multiplier": 1
    }
  ]
}
```


Then an example program using the config source above could look something like this.
Publishes and receives a json serialized message, prints received message to stdout (main.go):

```
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/pianoyeg94/go-rabbit-prototype"
)

const (
        configFile = "rabbit_conf.json"
	consumerName  = "test_consumer"
	publisherName = "test_publisher"
)

func main() {
	log := log.New(os.Stdout, "RABBIT : ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	if err := run(log); err != nil {
		log.Println("main: error:", err)
		os.Exit(1)
	}
}

func run(log *log.Logger) error {
	log.Println("main: initializing broker")

	// initialize rabbit app
	rbt, err := rabbit.New(configParser)
	if err != nil {
		return err
	}

	// register handler for consumer
	// will just print the delivery's body and acknowledge it
	rbt.SetConsumerHandler(consumerName, testConsumer)

	// create channels for gracefull shutdown,
	// caused either by a fatal error or an OS signal
	fatalErrors := make(chan error, 1)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// log non-fatal errors in the background
	go monitorErrLogs(log, rbt.Errors())

	// launch rabbit app:
	//  - will create all the necessary rabbit topology
	//  - launch consumers and publishers
	go func() {
		log.Println("main: started publishing and consuming")
		fatalErrors <- rbt.PublishAndConsume()
	}()

	// publish test message
	if err := testPublisher(rbt); err != nil {
		return err
	}

	// gracefull shutdown
	select {
	case err := <-fatalErrors:
		return errors.Wrap(err, "broker fatal error occured")
	case <-shutdown:
		log.Print("main: start broker shutdown")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rbt.Shutdown(ctx); err != nil {
			rbt.Close()
			return errors.Wrap(err, "could not stop broker gracefully")
		}
	}

	return nil
}

// parse any external config into rabbit app's config
func configParser(conf *rabbit.Config) error {
	file, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(file, conf); err != nil {
		return err
	}

	return nil
}

// print and acknowledge delivery
func testConsumer(delivery amqp.Delivery) error {
	fmt.Println("testConsumer received delivery: ", string(delivery.Body))
	return delivery.Ack(false)
}

// publish message to rabbit
func testPublisher(broker *rabbit.Rabbit) error {
	msg := "{\"message\": \"Hello World!\"}"
	pub, ok := broker.Publisher(publisherName)
	if !ok {
		return errors.New("publisher does not exist or wasn't configured")
	}
	return pub.Publish([]byte(msg), "", nil)
}

// log non-fatal errors comming from the rabbit app
func monitorErrLogs(log *log.Logger, errors <-chan error) {
	for err := range errors {
		log.Println("main: error:", err)
	}
}
```

Press Ctrl + C to initialize a gracefull shutdown of this script.
As a result you should get the following output in your console (well, not exactly, the timestamps will be different):

```
RABBIT : 2021/09/01 00:14:40.423481 main.go:33: main: initializing broker
RABBIT : 2021/09/01 00:14:40.455483 main.go:49: main: started publishing and consuming
testConsumer received delivery:  {"message": "Hello World!"}
RABBIT : 2021/09/01 00:14:42.142001 main.go:61: main: start broker shutdown
```
