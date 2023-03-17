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
	"hash/fnv"
	"log"
	"sort"
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

	activeSegmentPrefix = "active_segment_"
	segmentPrefix       = "segment_"

	committedOffsetKey = "committed_offset_"

	maxMessagesPerSegment = 100
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

func nodeFor(nodeIds []string, key string) string {
	h := fnv.New64()
	_, _ = h.Write([]byte(key))
	return nodeIds[h.Sum64()%uint64(len(nodeIds))]
}

func main() {

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	nodeIds := make([]string, 0)

	n.Handle(internal.InitReqType, func(msg maelstrom.Message) error {
		var body *internal.InitReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		nodeIds = body.NodeIds
		sort.Strings(nodeIds)
		return nil
	})

	n.Handle(sendType, func(msg maelstrom.Message) error {
		// - read body
		// - identify latest segment
		// - load latest segment
		// - if segment will overflow threshold, overwrite segment file and goto -1
		// - add item to end of segment with latest offset and CAS
		var body *sendBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		segment, err := getOrCreateLatestSegment(kv, body.Key, 5)
		if err != nil {
			return err
		}

		data, exists, err := readSegment(kv, body.Key, segment)
		if err != nil {
			return err
		} else if !exists {
			data = make([]int, 0)
		}

		if len(data) >= maxMessagesPerSegment {
			segment, err = advanceSegment(kv, body.Key, segment+1)
			data = make([]int, 0)
		}

		nextOffset := segment*maxMessagesPerSegment + len(data)
		data = append(data, body.Message)

		if err = writeSegment(kv, body.Key, segment, data); err != nil {
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
			if startOffset < 0 {
				startOffset = 0
			}
			startSegment := startOffset / maxMessagesPerSegment
			offset := startOffset % maxMessagesPerSegment
			for segment := startSegment; ; segment++ {
				data, exists, err := readSegment(kv, key, segment)
				if err != nil {
					return err
				} else if !exists {
					break
				} else {
					for ; offset < len(data); offset++ {
						response[key] = append(response[key], [2]int{segment*maxMessagesPerSegment + offset, data[offset]})
					}
					offset = 0
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

func buildActiveSegmentKey(key string) string {
	return activeSegmentPrefix + key
}

func buildSegmentKey(key string, id int) string {
	return segmentPrefix + key + fmt.Sprintf("_%010d", id)
}

func writeSegment(kv *maelstrom.KV, key string, segment int, data []int) error {
	return kv.CompareAndSwap(context.Background(), buildSegmentKey(key, segment), data[:len(data)-1], data, true)
}

func advanceSegment(kv *maelstrom.KV, key string, segment int) (int, error) {
	if err := kv.CompareAndSwap(context.Background(), buildActiveSegmentKey(key), segment, segment+1, true); err != nil {
		return 0, err
	}
	return segment + 1, nil
}

func getOrCreateLatestSegment(kv *maelstrom.KV, key string, maxRetry int) (int, error) {
	raw, err := kv.Read(context.Background(), buildActiveSegmentKey(key))
	if err != nil {
		var rpcError *maelstrom.RPCError
		if !errors.As(err, &rpcError) || rpcError.Code != maelstrom.KeyDoesNotExist {
			return 0, err
		}
		if err := kv.CompareAndSwap(context.Background(), buildActiveSegmentKey(key), nil, 0, true); err != nil {
			var rpcError *maelstrom.RPCError
			if !errors.As(err, &rpcError) || rpcError.Code != maelstrom.PreconditionFailed || maxRetry == 0 {
				return 0, err
			}
			return getOrCreateLatestSegment(kv, key, maxRetry-1)
		}
		return 0, nil
	} else {
		return raw.(int), nil
	}
}

func readSegment(kv *maelstrom.KV, key string, segment int) ([]int, bool, error) {
	segmentKey := buildSegmentKey(key, segment)
	raw, err := kv.Read(context.Background(), segmentKey)
	if err != nil {
		var rpcError *maelstrom.RPCError
		if !errors.As(err, &rpcError) || rpcError.Code != maelstrom.KeyDoesNotExist {
			return nil, false, err
		}
		return nil, false, nil
	} else {
		rawSlice := raw.([]any)
		data := make([]int, len(rawSlice))
		for i := 0; i < len(rawSlice); i++ {
			data[i] = int(rawSlice[i].(float64))
		}
		return data, true, nil
	}
}
