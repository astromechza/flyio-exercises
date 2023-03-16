package main

import (
	"context"
	"encoding/json"
	"flyio-exercises/internal"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync/atomic"
	"time"
)

const (
	addReqType   = "add"
	addRespType  = addReqType + "_ok"
	readReqType  = "read"
	readRespType = readReqType + "_ok"
)

type addReq struct {
	internal.SimpleReq
	Delta int64 `json:"delta"`
}

type readResp struct {
	internal.SimpleResp
	Value int64 `json:"value"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	localTotal := new(atomic.Int64)
	peerTotals := new(atomic.Int64)

	n.Handle(addReqType, func(msg maelstrom.Message) error {
		var body *addReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("Received add resp: %v\n", body)
		if body.Delta < 0 {
			return fmt.Errorf("refusing to add negative value")
		} else if body.Delta > 0 {
			localTotal.Add(body.Delta)
		}
		return n.Reply(msg, internal.SimpleResp{Type: addRespType})
	})

	n.Handle(readReqType, func(msg maelstrom.Message) error {
		log.Printf("Received read req")
		return n.Reply(msg, readResp{internal.SimpleResp{Type: readRespType}, localTotal.Load() + peerTotals.Load()})
	})

	pulse := func(lastLocalTotal int64) int64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		lt := localTotal.Load()
		if lt > lastLocalTotal {
			if err := kv.Write(ctx, n.ID(), lt); err != nil {
				log.Printf("err: %v", err)
			} else {
				lastLocalTotal = lt
			}
		}

		p := int64(0)
		for _, id := range n.NodeIDs() {
			if id != n.ID() {
				v, err := kv.ReadInt(ctx, id)
				if err != nil {
					log.Printf("err: %v", err)
					continue
				}
				p += int64(v)
			}
		}
		peerTotals.Store(p)
		return lastLocalTotal
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	done := make(chan bool)
	go func() {
		var llt int64
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				llt = pulse(llt)
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	done <- true
}
