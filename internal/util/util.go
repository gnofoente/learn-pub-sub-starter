package util

import (
	"os"
	"os/signal"
)

func DoOnSignal(fn func(), signals ...os.Signal) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, signals...)
	<-sigChan
	if fn != nil {
		fn()
	}
}
