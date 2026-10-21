package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

const connUrl string = "amqp://guest:guest@localhost:5672/"

func main() {
	log.Println("starting Peril server...")
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

	gamelogic.PrintServerHelp()

	queueName := routing.GameLogSlug
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		queueName,
		routing.GameLogSlug+"*",
		pubsub.QueueTypeDurable,
	)
	if err != nil {
		fmt.Printf("failed to declare %s: %v", queueName, err)
		return
	}
	fmt.Println("declared", queueName, "successfully")

loop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("sending pause message...")
			pubsub.PublishJSON(
				chn,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			fmt.Println("sent.")
			break
		case "resume":
			fmt.Println("sending resume message...")
			pubsub.PublishJSON(
				chn,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			fmt.Println("sent.")
			break
		case "quit":
			fmt.Println("exiting...")
			break loop
		default:
			fmt.Println(util.MessageWrongCommand)
		}
	}

	util.DoOnSignal(func() {
		log.Println("application gracefully stopped")
	}, os.Interrupt)
}
