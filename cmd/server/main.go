package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

const connUrl string = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("starting Peril server...")
	conn, err := amqp.Dial(connUrl)
	if err != nil {
		fmt.Println("failed to connect to message broker:", err)
		return
	}
	defer conn.Close()

	chn, err := conn.Channel()
	if err != nil {
		fmt.Println("failed to create channel")
		return
	}

	pubsub.PublishJSON(
		chn,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	)

	fmt.Println("connected succesfully")

	util.DoOnSignal(func() {
		fmt.Println("application gracefully stopped")
	}, os.Interrupt)
}
