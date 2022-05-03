package kafka

import (
	"context"
	"fmt"
	"github.com/bingxindan/bxd_queue/event"
	"testing"
)

func TestSend(t *testing.T) {
	sender, err := NewKafkaSender([]string{"0.0.0.0:29092"}, "topic_test1")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 50; i++ {
		send(sender)
	}

	_ = sender.Close()
}

func send(sender event.Sender) {
	msg := NewMessage("topic_test1", []byte("hello world"))
	err := sender.Send(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("key:%s, value:%s\n", msg.Key(), msg.Value())
}
