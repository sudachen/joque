package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sudachen/joque/go/server"
)

func main() {
	addr := flag.String("addr", "localhost:9100", "tcp address to listen")
	maxQueLength := flag.Int("qlen", 0, "max queue length, 0 means unlimited")
	maxTTL := flag.Int("jttl", 2, "max job TTL")
	flag.Parse()

	serv, err := server.StartJoqueServer(*addr, *maxQueLength, *maxTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "not started: %s", err.Error())
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.Signal(21))

	<-sigs
	serv.Stop()
}
