package main

import (
	"encoding/json"
	"log"

	"flyio-exercises/internal"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
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
)

type sendBody struct {
	internal.SimpleReq
	Key     string `json:"key"`
	Message int64  `json:"message"`
}

type sendOkBody struct {
	internal.SimpleResp
	Offset int64 `json:"offset"`
}

type pollBody struct {
	internal.SimpleReq
	Offsets map[string]int64 `json:"offsets"`
}

type pollOkBody struct {
	internal.SimpleResp
	Messages map[string][][]int64 `json:"msgs"`
}

type commitOffsetsBody struct {
	internal.SimpleReq
	Offsets map[string]int64 `json:"offsets"`
}

type listCommittedOffsetsBody struct {
	internal.SimpleReq
	Keys []string `json:"keys"`
}

type listCommittedOffsetsOkBody struct {
	internal.SimpleResp
	Offsets map[string]int64 `json:"offsets"`
}

func main() {

	n := maelstrom.NewNode()

	n.Handle(sendType, func(msg maelstrom.Message) error {
		var body *sendBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, sendOkBody{
			SimpleResp: internal.SimpleResp{Type: sendOkType},
			Offset:     0,
		})
	})

	n.Handle(pollType, func(msg maelstrom.Message) error {
		var body *pollBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, pollOkBody{
			SimpleResp: internal.SimpleResp{Type: pollOkType},
			Messages:   make(map[string][][]int64),
		})
	})

	n.Handle(commitOffsetsType, func(msg maelstrom.Message) error {
		var body *commitOffsetsBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, internal.SimpleResp{Type: commitOffsetsOkType})
	})

	n.Handle(listCommittedOffsetsType, func(msg maelstrom.Message) error {
		var body *listCommittedOffsetsBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return n.Reply(msg, listCommittedOffsetsOkBody{
			SimpleResp: internal.SimpleResp{Type: listCommittedOffsetsOkType},
			Offsets:    make(map[string]int64),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
