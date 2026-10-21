package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/util"
	amqp "github.com/rabbitmq/amqp091-go"
)

const connUrl string = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril client...")
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

	_, q, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("queue %v declared & bound\n", q.Name)

	gs := gamelogic.NewGameState(username)
	fmt.Println("a new game state was set")

	pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
		handlerPause(gs),
	)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			spawn(gs, words)
		case "move":
			move(gs, words)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println(util.MessageWrongCommand)
		}
	}
}

func spawn(gs *gamelogic.GameState, words []string) {
	err := gs.CommandSpawn(words)
	if err != nil {
		fmt.Printf("cannot spawn: %v\n", err)
	}
}

func move(gs *gamelogic.GameState, words []string) {
	_, err := gs.CommandMove(words)
	if err != nil {
		fmt.Printf("cannot move: %v\n", err)
		return
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}
