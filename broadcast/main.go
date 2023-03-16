package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	initReqType = "init"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle(initReqType, func(msg maelstrom.Message) error {
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
