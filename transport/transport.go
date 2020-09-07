package transport

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"mrcroxx.io/hermes/unit"
	"sync"
)

// errors
func errPodNotExists(podID uint64) error { return errors.New(fmt.Sprintf("pod %d not exists", podID)) }

func errNodeNotExists(nodeID uint64) error {
	return errors.New(fmt.Sprintf("node %d not exists", nodeID))
}

func errPodExists(podID uint64) error { return errors.New(fmt.Sprintf("pod %d already exists", podID)) }

func errNodeExists(nodeID uint64) error {
	return errors.New(fmt.Sprintf("node %d already exists", nodeID))
}

func errRaftNotExists(nodeID uint64) error {
	return errors.New(fmt.Sprintf("raft %d not exists, maybe node %d is not a local node", nodeID, nodeID))
}

// transport

type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type TCPServer interface {
	Init() error
	Close()
}

type TCPClient interface {
	Send(m raftpb.Message) error
	Close()
}

type Transport interface {
	Start() error
	Stop()
	Send(msgs []raftpb.Message)
	AddPod(podID uint64, url string) error
	RemovePod(podID uint64) error
	AddNode(podID uint64, nodeID uint64) error
	RemoveNode(nodeID uint64) error
}

type transport struct {
	podID       uint64
	url         string
	core        unit.Core
	snapshotter *snap.Snapshotter
	mux         sync.RWMutex         // RWMutex to maintain maps below
	npods       map[uint64]uint64    // node id -> pod id
	clients     map[uint64]TCPClient // pod id -> rpc client
	nodeSets    map[uint64][]uint64  // pod id -> node ids
	server      TCPServer
	done        chan struct{}
}

func NewTransport(podID uint64, url string, core unit.Core) Transport {
	return &transport{
		podID:    podID,
		url:      url,
		nodeSets: make(map[uint64][]uint64),
		npods:    make(map[uint64]uint64),
		clients:  make(map[uint64]TCPClient),
		done:     make(chan struct{}),
		core:     core,
	}
}

func (t *transport) Start() error {
	t.server = NewTCPServer(t.url, t.core)
	return t.server.Init()
}

func (t *transport) Stop() {
	for _, c := range t.clients {
		c.Close()
	}
	close(t.done)
	t.server.Close()
}

func (t *transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		t.mux.Lock()
		c, ok := t.clients[t.npods[m.To]]
		if ok {
			_ = c.Send(m)
		} else {
			// log.ZAPSugaredLogger().Warnf("Node %d metadata not found in pod %d, raft msg ignored.", m.To, t.podID)
		}
		t.mux.Unlock()
	}
}

func (t *transport) AddPod(podID uint64, url string) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.clients[podID]; exists {
		return errPodExists(podID)
	}
	c := NewTCPClient(url)
	t.clients[podID] = c
	t.nodeSets[podID] = []uint64{}
	return
}

func (t *transport) RemovePod(podID uint64) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if c, ok := t.clients[podID]; ok {
		defer c.Close()
		delete(t.clients, podID)
		delete(t.nodeSets, podID)
	} else {
		err = errPodNotExists(podID)
	}
	return
}

func (t *transport) AddNode(podID uint64, nodeID uint64) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.clients[podID]; exists {
		if _, ok := t.npods[nodeID]; !ok {
			t.npods[nodeID] = podID
			t.nodeSets[podID] = append(t.nodeSets[podID], nodeID)
		} else {
			return errNodeExists(nodeID)
		}
	} else {
		return errPodNotExists(podID)
	}
	return
}

func (t *transport) RemoveNode(nodeID uint64) (err error) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if _, exists := t.npods[nodeID]; exists {
		delete(t.npods, nodeID)
	} else {
		return errNodeNotExists(nodeID)
	}
	return
}
