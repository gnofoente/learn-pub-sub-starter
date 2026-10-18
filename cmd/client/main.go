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
	log.Println("Starting Peril client...")
	conn, err := amqp.Dial(connUrl)
	if err != nil {
		fmt.Printf("error connecting: %v", err)
		return
	}
	defer conn.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("error collecting username: %v", err)
		return
	}

	pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
	)

	util.DoOnSignal(func() {
		fmt.Println("goodbye...")
	}, os.Interrupt)
}
