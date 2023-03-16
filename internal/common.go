package internal

const (
	InitReqType  = "init"
	InitRespType = InitReqType + "_ok"
)

type SimpleReq struct {
	Type  string `json:"type"`
	MsgId int64  `json:"msg_id"`
}

// InitReq is the init message we get telling us how many nodes are taking part in the cluster.
// We use this to find the 'index' of our node and us this as a unique seed.
//
// {"type":"init","node_id":"n0","node_ids":["n0","n1","n2"],"msg_id":1}}
type InitReq struct {
	SimpleReq
	Id      string   `json:"node_id"`
	NodeIds []string `json:"node_ids"`
}

type SimpleResp struct {
	Type string `json:"type"`
}
