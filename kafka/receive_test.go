package kafka

import (
	"context"
	"fmt"
	"github.com/bingxindan/bxd_queue/event"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

func TestReceive(t *testing.T) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	receiver, err := NewKafkaReceiver([]string{"0.0.0.0:29092"}, "topic_test1", "topic_test")
	if err != nil {
		panic(err)
	}

	receive(receiver)

	<-sigs
	_ = receiver.Close()
}

func receive(receiver event.Receiver) {
	fmt.Println("start receiver")

	err := receiver.Receive(context.Background(), func(ctx context.Context, msg event.Event) error {
		fmt.Printf("key:%s, value:%s\n", msg.Key(), msg.Value())
		return nil
	})
	if err != nil {
		return
	}
}
