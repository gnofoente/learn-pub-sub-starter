package util

import (
	"os"
	"os/signal"
)

const MessageWrongCommand string = "I can't understand this command, try again."

func DoOnSignal(fn func(), signals ...os.Signal) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, signals...)

	go func() {
		<-sigChan
		if fn != nil {
			fn()
		}

		os.Exit(0)
	}()
}
