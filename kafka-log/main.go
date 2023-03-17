package main

/*
K-V structure:

KEY:

KEY_00000000:






*/

import (
	"context"
	"encoding/json"
	"errors"
	"flyio-exercises/internal"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

const (
	sendType   = "send"
	sendOkType = sendType + "_ok"

	pollType   = "poll"
	pollOkType = pollType + "_ok"

	commitOffsetsType   = "commit_offsets"
	commitOffsetsOkType = commitOffsetsType + "_ok"

	listCommittedOffsetsType   = "list_committed_offsets"
	listCommittedOffsetsOkType = listCommittedOffsetsType + "_ok"

	dataPrefix         = "data_"
	nextOffsetPrefix   = "next_offset_"
	committedOffsetKey = "committed_offset_"

	initialOffsetValue = 100000
)

type sendBody struct {
	internal.SimpleReq
	Key     string `json:"key"`
	Message int    `json:"msg"`
}

type sendOkBody struct {
	internal.SimpleResp
	Offset int `json:"offset"`
}

type pollBody struct {
	internal.SimpleReq
	Offsets map[string]int `json:"offsets"`
}

type pollOkBody struct {
	internal.SimpleResp
	Messages map[string][][2]int `json:"msgs"`
}

type commitOffsetsBody struct {
	internal.SimpleReq
	Offsets map[string]int `json:"offsets"`
}

type listCommittedOffsetsBody struct {
	internal.SimpleReq
	Keys []string `json:"keys"`
}

type listCommittedOffsetsOkBody struct {
	internal.SimpleResp
	Offsets map[string]int `json:"offsets"`
}

func main() {

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle(sendType, func(msg maelstrom.Message) error {
		// 1. read body
		// 2. read nextOffset key
		// 3. write payload
		// 4. update nextOffset key
		var body *sendBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		nextOffsetKey := nextOffsetPrefix + body.Key
		nextOffset := initialOffsetValue
		raw, err := kv.Read(context.Background(), nextOffsetKey)
		if err != nil {
			var rpcError *maelstrom.RPCError
			if !errors.As(err, &rpcError) || rpcError.Code != maelstrom.KeyDoesNotExist {
				return err
			}
		} else {
			nextOffset = raw.(int)
		}

		for ; ; nextOffset++ {
			if err := kv.CompareAndSwap(context.Background(), nextOffsetKey, nextOffset, nextOffset+1, true); err != nil {
				var rpcError *maelstrom.RPCError
				if !errors.As(err, &rpcError) || rpcError.Code != maelstrom.PreconditionFailed {
					return err
				}
				continue
			}
			break
		}

		// there's a possible problem here of 'orphaned' offsets which never get data written if the process crashes or halts here.
		// in the real world we may want to put these offsets onto a dead letter queue in order to "fence" out side effects and get
		// back our availability.

		offsetKey := dataPrefix + body.Key + fmt.Sprintf("_%010d", nextOffset)
		if err := kv.Write(context.Background(), offsetKey, body.Message); err != nil {
			return err
		}

		return n.Reply(msg, sendOkBody{
			SimpleResp: internal.SimpleResp{Type: sendOkType},
			Offset:     nextOffset,
		})
	})

	n.Handle(pollType, func(msg maelstrom.Message) error {
		// 1. read body
		// 2. for each offset given
		// 3. read next offset key and continue only if its < the next offset - 1
		// 4. if we hit any errors, stop and come back later since we know all keys should be continuous and we're
		//

		var body *pollBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := make(map[string][][2]int)
		for key, startOffset := range body.Offsets {
			if startOffset < initialOffsetValue {
				startOffset = initialOffsetValue
			}
			for offset := startOffset; ; offset++ {
				offsetKey := dataPrefix + key + fmt.Sprintf("_%010d", offset)
				raw, err := kv.Read(context.Background(), offsetKey)
				if err == nil {
					if value, ok := raw.(int); ok {
						response[key] = append(response[key], [2]int{offset, value})
					}
				} else {
					var rpcError *maelstrom.RPCError
					if errors.As(err, &rpcError) && rpcError.Code != maelstrom.KeyDoesNotExist {
						return err
					}
					break
				}
			}
		}

		return n.Reply(msg, pollOkBody{
			SimpleResp: internal.SimpleResp{Type: pollOkType},
			Messages:   response,
		})
	})

	n.Handle(commitOffsetsType, func(msg maelstrom.Message) error {
		var body *commitOffsetsBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for key, offset := range body.Offsets {
			// TODO: re-add validation that we can't go backwards
			if err := kv.Write(context.Background(), committedOffsetKey+key, offset); err != nil {
				return err
			}
		}
		return n.Reply(msg, internal.SimpleResp{Type: commitOffsetsOkType})
	})

	n.Handle(listCommittedOffsetsType, func(msg maelstrom.Message) error {
		// read committed offsets
		// filter only the included ones

		var body *listCommittedOffsetsBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := make(map[string]int)

		for _, key := range body.Keys {
			raw, err := kv.Read(context.Background(), committedOffsetKey+key)
			if err != nil {
				var rpcError *maelstrom.RPCError
				if errors.As(err, &rpcError) && rpcError.Code != maelstrom.KeyDoesNotExist {
					return err
				}
			} else {
				response[key] = raw.(int)
			}
		}

		return n.Reply(msg, listCommittedOffsetsOkBody{
			SimpleResp: internal.SimpleResp{Type: listCommittedOffsetsOkType},
			Offsets:    response,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
