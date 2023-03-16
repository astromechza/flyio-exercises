package main

import (
	"encoding/json"
	"flyio-exercises/internal"
	"fmt"
	"log"
	"sort"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// genResp is the response model we send back for each generate request.
type genResp struct {
	internal.SimpleResp
	Id int64 `json:"id"`
}

const (
	genReqType  = "generate"
	genRespType = "generate_ok"
)

func main() {
	n := maelstrom.NewNode()

	// On init, we setup the offset of our counter, as well as the value to add for each message,
	// the advantage of this is that we don't depend on the node id format, the disadvantage is that
	// we can't handle changes to the number of nodes. This is probably ok as long as we always re-use
	// node id's when we replace them.
	var counter = new(atomic.Int64)
	var nodeCount int64
	n.Handle(internal.InitReqType, func(msg maelstrom.Message) error {
		var body *internal.InitReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("Received init: %v\n", body)
		if len(body.NodeIds) == 0 {
			return fmt.Errorf("initialized with empty topology")
		}
		sort.Strings(body.NodeIds)
		nodeCount = int64(len(body.NodeIds))
		for i, id := range body.NodeIds {
			if body.Id == id {
				counter.Store(int64(i) + 1)
			}
		}
		if counter.Load() == 0 {
			return fmt.Errorf("node id '%v' not found in topology %v", body.Id, body.NodeIds)
		}
		return n.Reply(msg, internal.SimpleResp{Type: internal.InitRespType})
	})

	// on generate, we atomically advance our counter
	n.Handle(genReqType, func(msg maelstrom.Message) error {
		if counter.Load() == 0 {
			return fmt.Errorf("node has not been initialized")
		}
		newId := counter.Add(nodeCount)
		return n.Reply(msg, &genResp{internal.SimpleResp{Type: genRespType}, newId})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
