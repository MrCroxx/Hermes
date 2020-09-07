package store

import (
	"mrcroxx.io/hermes/pkg"
	"time"
)

type RaftRecord struct {
	ZoneID    uint64    `json:"ZoneID"`
	NodeID    uint64    `json:"NodeID"`
	PodID     uint64    `json:"PodID"`
	IsLeader  bool      `json:"IsLeader"`
	Heartbeat time.Time `json:"Heartbeat"`
	Extra     string    `json:"Extra"`
}

type RaftTable interface {
	All() (result []RaftRecord)
	Query(condition func(rr RaftRecord) bool) (result []RaftRecord)
	Insert(rrs []RaftRecord) int
	InsertIfNotExist(rrs []RaftRecord, condition func(rr RaftRecord) bool) int
	Delete(condition func(rr RaftRecord) bool) int
	Update(condition func(rr RaftRecord) bool, update func(rr *RaftRecord)) int
	GetSnapshot() ([]byte, error)
	RecoverFromSnapshot(snap []byte) error
}

type raftTable struct {
	records []RaftRecord
}

func NewRaftTable() RaftTable {
	return &raftTable{records: []RaftRecord{}}
}

func (rt *raftTable) All() (result []RaftRecord) {
	result = append(result, rt.records[:]...)
	return result
}

func (rt *raftTable) Query(condition func(rr RaftRecord) bool) (result []RaftRecord) {
	result = []RaftRecord{}
	for _, rr := range rt.records {
		if condition(rr) {
			result = append(result, rr)
		}
	}
	return result
}

func (rt *raftTable) Insert(rrs []RaftRecord) int {
	rt.records = append(rt.records, rrs...)
	return len(rrs)
}

func (rt *raftTable) InsertIfNotExist(rrs []RaftRecord, condition func(rr RaftRecord) bool) int {
	exists := false
	for _, rr := range rt.records {
		if condition(rr) {
			exists = true
			break
		}
	}
	if exists {
		return 0
	}
	rt.records = append(rt.records, rrs...)
	return len(rrs)
}

func (rt *raftTable) Delete(condition func(rr RaftRecord) bool) int {
	n := 0
	for i := 0; i < len(rt.records); {
		if condition(rt.records[i]) {
			rt.records = append(rt.records[:i], rt.records[i+1:]...)
			n++
		} else {
			i++
		}
	}
	return n
}

func (rt *raftTable) Update(condition func(rr RaftRecord) bool, update func(rr *RaftRecord)) int {
	n := 0
	for i, rr := range rt.records {
		if condition(rr) {
			update(&rt.records[i])
			n++
		}
	}
	return n
}

func (rt *raftTable) GetSnapshot() ([]byte, error) {
	return pkg.Encode(rt.records)
}

func (rt *raftTable) RecoverFromSnapshot(snap []byte) error {
	return pkg.Decode(snap, &rt.records)
}
