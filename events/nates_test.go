package events

import (
	"fmt"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	streaming "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

var DefaultTestOptions = server.Options{
	Host:           "127.0.0.1",
	Port:           4222,
	NoLog:          false,
	NoSigs:         false,
	MaxControlLine: 2048,
}

func TestPubSubNatsServer(t *testing.T) {
	server := test.RunServer(&DefaultTestOptions)
	defer server.Shutdown()

	wg := sync.WaitGroup{}
	wg.Add(2)
	//docker pull nats-streaming
	nc, _ := nats.Connect(nats.DefaultURL)

	// Simple Async Subscriber. Setup a subscriber before publish. There is no message guarantees on
	nc.Subscribe("foo", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		wg.Done()
	})

	// Simple Publisher
	nc.Publish("foo", []byte("Foo"))
	nc.Publish("foo", []byte("Bar"))
	wg.Wait()
}

func TestPubSubNatsPublishFirst(t *testing.T) {
	server := test.RunServer(&DefaultTestOptions)
	defer server.Shutdown()

	wg := sync.WaitGroup{}
	wg.Add(2)
	//docker pull nats-streaming
	nc, _ := nats.Connect(nats.DefaultURL)

	var msgCount count32
	// Simple Publisher
	nc.Publish("foo", []byte("Foo"))
	nc.Publish("foo", []byte("Bar"))

	// Simple Async Subscriber. Setup a subscriber before publish. There is no message guarantees on
	nc.Subscribe("foo", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		msgCount.increment()
		wg.Done()
	})

	go func() {
		time.Sleep(10 * time.Second)
		wg.Add(-2)
	}()

	wg.Wait()

	var expected int32
	expected = 2
	assert.Equal(t, expected, msgCount.get(), "Did not receive enough messages")
}

func TestNatsStreaming(t *testing.T) {
	clusterId := "my-cluster"

	streamServer, err := streaming.RunServer("my-cluster")
	require.NoError(t, err)

	publisher, err := stan.Connect(clusterId, "publisher1")
	replaySubscriber, err := stan.Connect(clusterId, "replaySubsciber")
	newEventSubscriber, err := stan.Connect(clusterId, "subscriber1")

	defer streamServer.Shutdown()
	defer publisher.Close()
	defer replaySubscriber.Close()

	wg := sync.WaitGroup{}
	wg.Add(3)
	startTime := time.Now()
	publisher.Publish("foo", []byte("Foo"))
	publisher.Publish("foo", []byte("Bar"))

	//Receives messages from beginning of time. Wild cards dont work on string just on tokens
	replaySubscriber.Subscribe("foo", func(m *stan.Msg) {
		fmt.Printf("[From beginning of time] Received a message: %s\n", string(m.Data))
		wg.Done()
	}, stan.StartAtTime(startTime))

	//Default receiver
	newEventSubscriber.Subscribe("foo", func(m *stan.Msg) {
		fmt.Printf("[New Event Subscriber] Received a message: %s\n", string(m.Data))
	})

	publisher.Publish("foo", []byte("After"))

	wg.Wait()
	time.Sleep(5 * time.Second)
}
