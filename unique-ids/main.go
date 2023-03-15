package main

import (
	"encoding/json"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// initReq is the init message we get telling us how many nodes are taking part in the cluster.
// We use this to find the 'index' of our node and us this as a unique seed.
//
// {"type":"init","node_id":"n0","node_ids":["n0","n1","n2"],"msg_id":1}}
type initReq struct {
	Id      string   `json:"node_id"`
	NodeIds []string `json:"node_ids"`
	MsgId   int64    `json:"msg_id"`
}

// genResp is the response model we send back for each generate request.
type genResp struct {
	Type string `json:"type"`
	Id   int64  `json:"id"`
}

const genRespType = "generate_ok"

func main() {
	n := maelstrom.NewNode()

	// on init, we setup the offset of our counter, as well as the value to add for each message
	var counter int64
	var nodeCount int64
	n.Handle("init", func(msg maelstrom.Message) error {
		var body *initReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("Received init: %v\n", body)
		nodeCount = int64(len(body.NodeIds))
		for i, id := range body.NodeIds {
			if body.Id == id {
				counter = int64(i)
			}
		}
		return nil
	})

	// on generate, we atomically advance our counter
	n.Handle("generate", func(msg maelstrom.Message) error {
		newId := atomic.AddInt64(&counter, nodeCount)
		return n.Reply(msg, &genResp{genRespType, newId})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
