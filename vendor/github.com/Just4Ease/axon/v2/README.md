# axon

## How to use

```go
package main

import (
	"github.com/Just4Ease/axon/v2/options"
	"github.com/Just4Ease/axon/v2/systems/jetstream"
	"log"
	"time"
)

func main() {

	var topic = "test-topic" // for random topic name.
	var store, _ = jetstream.Init(options.Options{
		ServiceName: "test-event-store",
		Address:     "nats://localhost:4222",
	})
	

	timer := time.AfterFunc(3*time.Second, func() {
		if err := store.Subscribe(topic, func(event axon.Event) {
			data := event.Data()

			eventTopic := event.Topic()

			if topic != eventTopic {
				log.Fatalf("Event topic is not the same as subscription topic. Why?: Expected %s, instead got: %s \n", topic, eventTopic)
				return
			}

			log.Printf("Received data: %s on topic: %s \n", string(data), eventTopic)
			event.Ack() // Acknowledge event.
			return
		}); err != nil {
			log.Fatalf("Failed to subscribe to topic: %s, with the following error: %v \n", topic, err)
			return
		}
	})

	defer timer.Stop()

	if err := store.Publish(topic, []byte("Hello World!")); err != nil {
		log.Fatalf("Could not publish message to %s", topic)
	}

}
```

```shell script
# For tests

$ make test 
```
