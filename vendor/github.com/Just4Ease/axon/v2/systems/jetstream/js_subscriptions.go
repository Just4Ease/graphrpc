package jetstream

import (
	"fmt"
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/axon/v2/messages"
	"github.com/Just4Ease/axon/v2/options"
	"github.com/gookit/color"
	"github.com/nats-io/nats.go"
	"log"
	"strings"
	"time"
)

type subscription struct {
	topic       string
	cb          axon.SubscriptionHandler
	axonOpts    *options.Options
	subOptions  *options.SubscriptionOptions
	store       *natsStore
	closeSignal chan bool
	serviceName string
}

func (s *subscription) mountSubscription() error {
	errChan := make(chan error)

	cbHandler := func(m *nats.Msg) {
		var msg messages.Message
		if err := s.store.msh.Unmarshal(m.Data, &msg); err != nil {
			errChan <- err
			return
		}

		event := newEvent(m, msg)
		go s.cb(event)
	}

	topic := fmt.Sprintf("%s-%s", s.topic, s.subOptions.ExpectedSpecVersion())

	durableStore := strings.ReplaceAll(fmt.Sprintf("%s-%s", s.serviceName, topic), ".", "-")

	// Without JetStream, use just nats.
	if s.subOptions.IsStreamingDisabled() || !s.store.jsmEnabled {
		go func(s *subscription, errChan chan<- error) {
			var err error
			var sub *nats.Subscription
			switch s.subOptions.SubscriptionType() {
			case options.Shared:
				sub, err = s.store.nc.QueueSubscribe(topic, durableStore, cbHandler)
				if err != nil {
					errChan <- err
					return
				}
			case options.KeyShared:
				if sub, err = s.store.nc.Subscribe(topic, cbHandler); err != nil {
					errChan <- err
					return
				}
			default:
				log.Fatal("only Shared and KeyShared subscription can work if jetstream is not enabled on nats-server")
			}

			<-s.closeSignal
			if err := sub.Drain(); err != nil {
				log.Printf("failed to drain subscription with the following error before exitting: %v", err)
				errChan <- err
			}
		}(s, errChan)
		goto holdWatcher
	}

	// With JetStream use JetStream
	go func(s *subscription, errChan chan<- error) {
		var err error
		var sub *nats.Subscription
		switch s.subOptions.SubscriptionType() {
		case options.Failover:

			break
		case options.Exclusive:
			consumer, err := s.store.jsc.AddConsumer(s.serviceName, &nats.ConsumerConfig{
				Durable: durableStore,
				//DeliverSubject: nats.NewInbox(),
				DeliverPolicy: nats.DeliverLastPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				MaxDeliver:    s.subOptions.MaxRedelivery(),
				ReplayPolicy:  nats.ReplayOriginalPolicy,
				MaxAckPending: 20000,
				FlowControl:   false,
				//AckWait:         0,
				//RateLimit:       0,
				//Heartbeat:       0,
			})
			if err != nil {
				errChan <- err
				return
			}

			if sub, err = s.store.jsc.QueueSubscribe(consumer.Name, durableStore, cbHandler, nats.Durable(durableStore),
				nats.DeliverLast(),
				nats.EnableFlowControl(),
				nats.BindStream(s.serviceName),
				nats.MaxAckPending(20000000),
				nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(s.subOptions.MaxRedelivery())); err != nil {
				errChan <- err
				return
			}

		case options.Shared:
			sub, err = s.store.jsc.QueueSubscribe(topic,
				durableStore,
				cbHandler,
				nats.Durable(durableStore),
				nats.DeliverLast(),
				nats.BindStream(s.serviceName),
				nats.MaxAckPending(20000000),
				nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(s.subOptions.MaxRedelivery()))
			if err != nil {
				errChan <- err
				return
			}
		case options.KeyShared:
			if sub, err = s.store.jsc.Subscribe(topic,
				cbHandler,
				nats.Durable(durableStore),
				nats.DeliverLast(),
				nats.EnableFlowControl(),
				nats.BindStream(s.serviceName),
				nats.MaxAckPending(20000000),
				nats.ManualAck(),
				nats.ReplayOriginal(),
				nats.MaxDeliver(s.subOptions.MaxRedelivery())); err != nil {
				errChan <- err
				return
			}
		}

		<-s.closeSignal
		if err := sub.Drain(); err != nil {
			log.Printf("failed to drain subscription with the following error before exitting: %v", err)
			errChan <- err
		}
	}(s, errChan)

holdWatcher:
	color.Green.Printf("⚡️ Subscription: %s (%s) 🤘🏼 \n", s.topic, s.subOptions.ExpectedSpecVersion())
	select {
	case <-s.subOptions.Context().Done():
		s.closeSignal <- true
		return nil
	case err := <-errChan:
		return err
	}
}

func (s *subscription) runSubscriptionHandler() {
start:
	if err := s.mountSubscription(); err != nil {
		log.Printf("creating a consumer returned error: %v. Reconnecting in 3secs...", err)
		time.Sleep(3 * time.Second)
		goto start
	}
}

func (s *natsStore) addSubscriptionToSubscriptionPool(sub *subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic := fmt.Sprintf("%s-%s", sub.topic, sub.subOptions.ExpectedSpecVersion())

	if _, ok := s.subscriptions[topic]; ok {
		log.Fatalf("there is already an existing subscription registered to this topic: %s", sub.topic)
	}

	s.subscriptions[topic] = sub
	return nil
}

func (s *natsStore) Subscribe(topic string, handler axon.SubscriptionHandler, opts ...options.SubscriptionOption) error {
	subOptions, err := options.DefaultSubOptions(opts...)
	if err != nil {
		return err
	}

	sub := &subscription{
		topic:       topic,
		cb:          handler,
		axonOpts:    &s.opts,
		subOptions:  subOptions,
		store:       s,
		serviceName: s.opts.ServiceName,
	}

	return s.addSubscriptionToSubscriptionPool(sub)
}

func (s *subscription) close() {
	s.closeSignal <- true
}
