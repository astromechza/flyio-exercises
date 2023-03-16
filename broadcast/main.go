package main

import (
	"encoding/json"
	"flyio-exercises/internal"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	broadcastReqType  = "broadcast"
	broadcastRespType = broadcastReqType + "_ok"
	readReqType       = "read"
	readRespType      = readReqType + "_ok"
	topologyReqType   = "topology"
	topologyRespType  = topologyReqType + "_ok"
)

type broadcastReq struct {
	internal.SimpleReq
	Message int64 `json:"message"`
}

type topologyReq struct {
	internal.SimpleReq
	Topology map[string][]string `json:"topology"`
}

type readResp struct {
	internal.SimpleResp
	Messages []int64 `json:"messages"`
}

func main() {
	n := maelstrom.NewNode()

	var neighbours *[]string
	{
		tmp := make([]string, 0)
		neighbours = &tmp
	}
	lock := new(sync.RWMutex)
	// We're going to store 2 forms of our data, a log of the appended items, and a uniqueness lookup to ensure we
	// aren't storing duplicates. This allows us to return stable sub-sets of the data.
	dataLog := make([]int64, 0, 128)
	data := make(map[int64]bool)

	n.Handle(internal.InitReqType, func(msg maelstrom.Message) error {
		var body *internal.InitReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("Received init: %v\n", body)
		if len(body.NodeIds) == 0 {
			return fmt.Errorf("initialized with empty topology")
		}

		tmp := make([]string, 0, len(body.NodeIds))
		for _, id := range body.NodeIds {
			if id != n.ID() {
				tmp = append(tmp, id)
			}
		}
		neighbours = &tmp
		log.Printf("Updated neighbouring nodes to %v", neighbours)
		return n.Reply(msg, internal.SimpleResp{Type: internal.InitRespType})
	})

	n.Handle(topologyReqType, func(msg maelstrom.Message) error {
		var body *topologyReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("Received topology update: %v\n", body)

		if raw, ok := body.Topology[n.ID()]; ok {
			tmp := make([]string, len(raw))
			copy(tmp, raw)
			neighbours = &tmp
			log.Printf("Updated neighbouring nodes to %v", neighbours)
		}
		return n.Reply(msg, internal.SimpleResp{Type: topologyRespType})
	})

	n.Handle(readReqType, func(msg maelstrom.Message) error {
		lock.RLock()
		defer lock.RUnlock()
		tmp := make([]int64, len(dataLog))
		copy(tmp, dataLog)
		return n.Reply(msg, readResp{internal.SimpleResp{Type: readRespType}, tmp})
	})

	n.Handle(broadcastReqType, func(msg maelstrom.Message) error {
		var body *broadcastReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		lock.Lock()
		defer lock.Unlock()
		if _, ok := data[body.Message]; !ok {
			dataLog = append(dataLog, body.Message)
			data[body.Message] = true
		}

		return n.Reply(msg, internal.SimpleResp{Type: broadcastRespType})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
