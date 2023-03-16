package main

import (
	"encoding/json"
	"flyio-exercises/internal"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	broadcastReqType  = "broadcast"
	broadcastRespType = broadcastReqType + "_ok"
	readReqType       = "read"
	readRespType      = readReqType + "_ok"
	topologyReqType   = "topology"
	topologyRespType  = topologyReqType + "_ok"

	syncReqType  = "sync"
	syncRespType = syncReqType + "_ok"
)

type broadcastReq struct {
	internal.SimpleReq
	Message int64 `json:"message"`
}

type readResp struct {
	internal.SimpleResp
	Messages []int64 `json:"messages"`
}

type syncReq struct {
	internal.SimpleReq
	Messages []int64 `json:"messages"`
	Position int
}

type syncResp struct {
	internal.SimpleResp
	Position int
}

func main() {
	n := maelstrom.NewNode()

	neighbours := new(sync.Map)
	lock := new(sync.RWMutex)
	// We're going to store 2 forms of our data, a log of the appended items, and a uniqueness lookup to ensure we
	// aren't storing duplicates. This allows us to return stable sub-sets of the data.
	dataLog := make([]int64, 0, 128)
	data := make(map[int64]bool)

	n.Handle(topologyReqType, func(msg maelstrom.Message) error {
		if err := n.Reply(msg, internal.SimpleResp{Type: topologyRespType}); err != nil {
			log.Printf("ignoring failed topology ok reply")
		}
		return nil
	})

	n.Handle(readReqType, func(msg maelstrom.Message) error {
		log.Printf("Received read req")
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
		log.Printf("Received broadcast req: %v\n", body)

		lock.Lock()
		defer lock.Unlock()
		if _, ok := data[body.Message]; !ok {
			dataLog = append(dataLog, body.Message)
			data[body.Message] = true
		}

		return n.Reply(msg, internal.SimpleResp{Type: broadcastRespType})
	})

	// When we get some data from a peer, we update any missing data and send back a sync ok reply for that position
	n.Handle(syncReqType, func(msg maelstrom.Message) error {
		var body *syncReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("Received sync req: %v\n", body)

		lock.Lock()
		defer lock.Unlock()
		for _, message := range body.Messages {
			if _, ok := data[message]; !ok {
				dataLog = append(dataLog, message)
				data[message] = true
			}
		}

		return n.Send(msg.Src, syncResp{
			internal.SimpleResp{Type: syncRespType},
			body.Position + len(body.Messages),
		})
	})

	// When we get a sync-ok message from a peer, we update its position in our neighbours so that we don't send them
	// the same data again
	n.Handle(syncRespType, func(msg maelstrom.Message) error {
		var body *syncResp
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		log.Printf("Received sync resp: %v\n", body)

		last, ok := neighbours.Load(msg.Src)
		if ok {
			if body.Position > last.(int) {
				neighbours.CompareAndSwap(msg.Src, last.(int), body.Position)
			}
		} else {
			neighbours.Store(msg.Src, body.Position)
		}
		return nil
	})

	pulseFraction := 0.1
	pulse := func() {
		dl := len(dataLog)

		ids := make([]string, len(n.NodeIDs()))
		copy(ids, n.NodeIDs())
		rand.Shuffle(len(ids), func(i, j int) {
			t := ids[i]
			ids[i] = ids[j]
			ids[j] = t
		})
		ids = ids[0:int(math.Ceil(pulseFraction*float64(len(ids))))]
		lock.RLock()
		defer lock.RUnlock()

		for _, id := range ids {
			if id == n.ID() {
				continue
			}
			oldPos := 0
			if raw, ok := neighbours.Load(id); ok {
				oldPos = raw.(int)
			}
			if oldPos < dl {
				missingMessageCount := dl - oldPos
				missingMessages := make([]int64, missingMessageCount)
				for i := 0; i < missingMessageCount; i++ {
					missingMessages[i] = dataLog[oldPos+i]
				}

				log.Printf("sending %v items to %v", missingMessageCount, id)
				if err := n.Send(id, syncReq{
					internal.SimpleReq{Type: syncReqType},
					missingMessages,
					oldPos,
				}); err != nil {
					log.Printf("temporary error sending reply: %v", err)
				}
			}
		}

		neighbours.Range(func(key, value any) bool {

			return true
		})
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				pulse()
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	done <- true
}
