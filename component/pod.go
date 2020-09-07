package component

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/store"
	"mrcroxx.io/hermes/transport"
	"mrcroxx.io/hermes/unit"
	"path"
	"sync"
	"time"
)

var (
	errMetaNodeNotExist = errors.New("metaNode node in this pod not exist")
)

// pod
type pod struct {
	podID                   uint64                   // pod id
	pods                    map[uint64]string        // pod id -> url
	storageDir              string                   // path to storage
	transport               transport.Transport      // transport engine
	errC                    chan<- error             // send pod errors
	metaNode                unit.MetaNode            // mate node
	nodes                   map[uint64]unit.DataNode // node id -> data node
	triggerSnapshotEntriesN uint64                   // entries count to trigger raft snapshot
	snapshotCatchUpEntriesN uint64                   // entries count for slow follower catch up before compacting
	metaZoneOffset          uint64                   // zone id and node id offset for metaNode zone
	cfg                     config.HermesConfig      // hermes config for pod constructing
	ackCs                   map[uint64]<-chan uint64 // node id -> ack signal channel
	mux                     sync.Mutex
	wakeTicker              *time.Ticker
}

func NewPod(
	cfg config.HermesConfig,
	errC chan<- error,
// cmdC <-chan command.PodCMD,
) unit.Pod {
	// init pod struct
	p := &pod{
		cfg:                     cfg,
		podID:                   cfg.PodID,
		pods:                    cfg.Pods,
		storageDir:              cfg.StorageDir,
		triggerSnapshotEntriesN: cfg.TriggerSnapshotEntriesN,
		snapshotCatchUpEntriesN: cfg.SnapshotCatchUpEntriesN,
		metaZoneOffset:          cfg.MetaZoneOffset,
		errC:                    errC,
		nodes:                   make(map[uint64]unit.DataNode),
		wakeTicker:              time.NewTicker(time.Second * 30),
	}
	p.transport = transport.NewTransport(cfg.PodID, cfg.Pods[cfg.PodID], p)

	// init transport
	// err returns `nil` if transport is ready
	log.ZAPSugaredLogger().Debugf("starting transport ...")
	if err := p.transport.Start(); err != nil {
		p.errC <- err
		return nil
	}
	log.ZAPSugaredLogger().Debugf("transport started.")

	p.connectCluster()
	p.startMetaNode()
	go p.wakeTick()

	return p
}

func (p *pod) wakeTick() {
	for _ = range p.wakeTicker.C {
		if p.metaNode == nil {
			continue
		}
		for _, rr := range p.metaNode.OfflineNodes() {
			go p.wakeUpNode(rr)
		}
	}
}

func (p *pod) wakeUpNode(rr store.RaftRecord) {
	if p.metaNode == nil {
		return
	}
	peerRRs := p.metaNode.LookUpZoneRRs(rr.ZoneID)
	peers := make(map[uint64]uint64)
	for _, prr := range peerRRs {
		peers[prr.PodID] = prr.NodeID
	}
	log.ZAPSugaredLogger().Infof("wake up : %d", rr.NodeID)
	p.StartDataNode(rr.ZoneID, rr.NodeID, peers)
}

func (p *pod) Stop() {
	p.transport.Stop()
}

func (p *pod) All() ([]store.RaftRecord, error) {
	if p.metaNode == nil {
		return nil, errMetaNodeNotExist
	}
	return p.metaNode.All(), nil
}

func (p *pod) AddRaftZone(zoneID uint64, nodes map[uint64]uint64) error {
	if p.metaNode == nil {
		return errMetaNodeNotExist
	}
	return p.metaNode.AddRaftZone(zoneID, nodes)
}

func (p *pod) TransferLeadership(zoneID uint64, nodeID uint64) error {
	if p.metaNode == nil {
		return errMetaNodeNotExist
	}
	return p.metaNode.TransferLeadership(zoneID, nodeID)
}

func (p *pod) ReplayDataZone(zoneID uint64, index uint64) {
	if p.metaNode == nil {
		return
	}
	p.metaNode.ProposeNotifyReplayDataZone(zoneID, index)
}

func (p *pod) WakeUpNode(nodeID uint64) {
	if p.metaNode == nil {
		return
	}
	if rr, ok := p.metaNode.LookUpDeadNodeRR(nodeID); ok {
		p.wakeUpNode(rr)
	}
}

func (p *pod) connectCluster() {
	for podID, url := range p.pods {
		if err := p.transport.AddPod(podID, url); err != nil {
			p.errC <- err
		}
	}
}

func (p *pod) startMetaNode() {
	nodeID := p.podID + p.metaZoneOffset
	peers := make(map[uint64]uint64)
	for podID, _ := range p.pods {
		peers[podID] = podID + p.metaZoneOffset
	}

	p.metaNode = NewMetaNode(MetaNodeConfig{
		Core:                    p,
		ZoneID:                  p.metaZoneOffset,
		NodeID:                  nodeID,
		PodID:                   p.podID,
		Peers:                   peers,
		Join:                    false,
		StorageDir:              path.Join(p.storageDir, fmt.Sprintf("%d", nodeID)),
		TriggerSnapshotEntriesN: p.triggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: p.snapshotCatchUpEntriesN,
		Transport:               p.transport,
		DoLeadershipTransfer:    p.doLeadershipTransfer,
	})
	if p.metaNode == nil {
		log.ZAPSugaredLogger().Fatalf("Failed to create metaNode node.")
		panic(nil)
	}
}

func (p *pod) StartDataNode(zoneID uint64, nodeID uint64, peers map[uint64]uint64) {
	p.mux.Lock()
	defer p.mux.Unlock()
	d := NewDataNode(DataNodeConfig{
		Core:                    p,
		ZoneID:                  zoneID,
		NodeID:                  nodeID,
		Peers:                   peers,
		Join:                    false,
		StorageDir:              path.Join(p.storageDir, fmt.Sprintf("%d", nodeID)),
		TriggerSnapshotEntriesN: p.triggerSnapshotEntriesN,
		SnapshotCatchUpEntriesN: p.snapshotCatchUpEntriesN,
		Transport:               p.transport,
		PushDataURL:             p.cfg.PushDataURL,
		MaxPersistN:             p.cfg.MaxPersistN,
		MaxPushN:                p.cfg.MaxPushN,
		MaxCacheN:               p.cfg.MaxCacheN,
		NotifyLeaderShip:        p.metaNode.NotifyLeadership,
		Heartbeat:               p.metaNode.Heartbeat,
	})
	if d == nil {
		log.ZAPSugaredLogger().Error("Error raised when add data node")
	}
	p.nodes[nodeID] = d
}

func (p *pod) doLeadershipTransfer(podID uint64, old uint64, transferee uint64) {
	log.ZAPSugaredLogger().Debugf("do leadership transfer : pod %d old %d transferee %d.", podID, old, transferee)
	if podID != p.podID {
		return
	}
	if p.metaNode != nil && p.metaNode.NodeID() == transferee {
		p.metaNode.DoLead(old)
		return
	}
	if n, exists := p.nodes[transferee]; exists {
		n.DoLead(old)
		return
	}
}

func (p *pod) Metadata() (*unit.Metadata, error) {
	rr, err := p.All()
	if err != nil {
		return nil, err
	}
	return &unit.Metadata{
		Config:      p.cfg,
		RaftRecords: rr,
	}, nil
}

func (p *pod) InitMetaZone() error {
	zid := p.metaZoneOffset
	nodes := make(map[uint64]uint64)
	nid := uint64(0)
	for pid, _ := range p.pods {
		if nid == 0 {
			nid = pid + p.metaZoneOffset
		}
		nodes[pid+p.metaZoneOffset] = pid
	}
	if err := p.AddRaftZone(zid, nodes); err != nil {
		return err
	}
	t := time.NewTicker(time.Second * 3)
	go func() {
		<-t.C
		p.TransferLeadership(zid, nid)
	}()
	return nil
}

func (p *pod) PodID() uint64 { return p.podID }

func (p *pod) RaftProcessor(nodeID uint64) func(ctx context.Context, m raftpb.Message) error {
	if p.metaNode == nil {
		return nil
	}
	if p.metaNode.NodeID() == nodeID {
		return p.metaNode.RaftProcessor()
	}
	if d, exists := p.nodes[nodeID]; exists {
		return d.RaftProcessor()
	}
	return nil
}

func (p *pod) LookUpLeader(zoneID uint64) (nodeID uint64, podID uint64) {
	if p.metaNode == nil {
		return 0, 0
	}
	return p.metaNode.LookUpLeader(zoneID)
}

func (p *pod) AppendData(nodeID uint64, ts int64, data []string, callback func(int64)) bool {
	if d, exists := p.nodes[nodeID]; exists {
		d.RegisterACKCallback(ts, callback)
		d.ProposeAppend(ts, data)
		return true
	}
	return false
}

func (p *pod) LookUpNextFreshIndex(nodeID uint64) uint64 {
	if d, exists := p.nodes[nodeID]; exists {
		return d.NextFreshIndex()
	}
	return 0
}

func (p *pod) NotifyReplayDataZone(zoneID uint64, index uint64) {
	leaderID, _ := p.LookUpLeader(zoneID)
	if d, exists := p.nodes[leaderID]; exists {
		d.ProposeReplay(index)
	}
}
