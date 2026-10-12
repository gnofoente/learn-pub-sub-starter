package main

import (
	"fmt"
	"os"
	"os/signal"

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

	fmt.Println("connected succesfully")

	doOnSignal(func() {
		fmt.Println("application gracefully stopped")
	}, os.Interrupt)
}

func doOnSignal(fn func(), signals ...os.Signal) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, signals...)
	<-sigChan
	fn()
}
