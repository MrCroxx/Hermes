package cmd

import (
	"mrcroxx.io/hermes/store"
	"time"
)

type METACMDTYPE int
type DATACMDTYPE int

var (
	HEIHEIHEI = "asd"
)

const (
	METACMDTYPE_RAFT_ADDZONE METACMDTYPE = iota
	METACMDTYPE_RAFT_NOTIFY_LEADERSHIP
	METACMDTYPE_RAFT_TRANSFER_LEADERATHIP
	METACMDTYPE_NODE_HEARTBEAT
	METACMDTYPE_DATA_REPLAY
)
const (
	DATACMDTYPE_APPEND DATACMDTYPE = iota
	DATACMDTYPE_CACHE
	DATACMDTYPE_PERSIST
	DATACMDTYPE_REPLAY
)

type MetaCMD struct {
	Type      METACMDTYPE
	Records   []store.RaftRecord
	ZoneID    uint64
	NodeID    uint64
	PodID     uint64
	OldNodeID uint64
	N         uint64
	Time      time.Time
	Extra     []byte
}

type DataCMD struct {
	Type DATACMDTYPE
	Data []string
	TS   int64
	N    uint64
	Time time.Time
}

type HermesProducerCMD struct {
	ZoneID uint64
	TS     int64
	Index  uint64
	Data   []string
}

type HermesProducerRSP struct {
	Err   string // error
	TS    int64
	PodID uint64 // pod id for leader node now
	Index uint64
}

type HermesConsumerCMD struct {
	ZoneID     uint64   `json:"zone_id"`
	FirstIndex uint64   `json:"first_index"`
	Data       []string `json:"data"`
}

type HermesConsumerRSP struct {
	ACK uint64 `json:"ack"`
}
