package component

import (
	"context"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"math/rand"
	cmd2 "mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/config"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"mrcroxx.io/hermes/transport"
	"mrcroxx.io/hermes/unit"
	"os"
	"path"
	"time"
)

type raftEngine struct {
	proposeC      <-chan []byte            // proposed messages
	confChangeC   <-chan raftpb.ConfChange // proposed raft config changes
	commitC       chan<- *[]byte           // committed entries
	advanceC      <-chan struct{}          // state machine advance apply
	errorC        chan<- error             // errors from raft session
	nodeID        uint64                   // node id
	peers         []uint64                 // node ids in the same raft zone
	join          bool                     // node is joining an existing cluster
	storageDir    string                   // path to storage
	lastIndex     uint64                   // index of log at start
	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	node                    raft.Node
	storage                 *raft.MemoryStorage
	wal                     *wal.WAL
	snapshotter             *snap.Snapshotter
	snapshotterReadyC       chan *snap.Snapshotter
	triggerSnapshotEntriesN uint64
	snapshotCatchUpEntriesN uint64
	transport               transport.Transport

	stopc chan struct{}

	getSnapshot      func() ([]byte, error)
	notifyLeadership func(nodeID uint64)
}

type RaftEngineConfig struct {
	NodeID                  uint64
	Peers                   []uint64
	Join                    bool
	Transport               transport.Transport
	StorageDir              string
	ProposeC                <-chan []byte
	ConfChangeC             <-chan raftpb.ConfChange
	TriggerSnapshotEntriesN uint64
	SnapshotCatchUpEntriesN uint64
	GetSnapshot             func() ([]byte, error)
	NotifyLeadership        func(nodeID uint64)
	MetaNode                unit.MetaNode
	DataNode                unit.DataNode
}

type RaftEngine struct {
	CommitC           <-chan *[]byte
	ErrorC            <-chan error
	SnapshotterReadyC <-chan *snap.Snapshotter
	DoLead            func(old uint64)
	AdvanceC          chan<- struct{}
	RaftProcessor     func(ctx context.Context, m raftpb.Message) error
}

func NewRaftEngine(cfg RaftEngineConfig) RaftEngine {
	cc := make(chan *[]byte)
	ec := make(chan error)
	ac := make(chan struct{})
	re := &raftEngine{
		nodeID:                  cfg.NodeID,
		peers:                   cfg.Peers,
		join:                    cfg.Join,
		proposeC:                cfg.ProposeC,
		confChangeC:             cfg.ConfChangeC,
		commitC:                 cc,
		errorC:                  ec,
		advanceC:                ac,
		transport:               cfg.Transport,
		storageDir:              cfg.StorageDir,
		getSnapshot:             cfg.GetSnapshot,
		triggerSnapshotEntriesN: cfg.TriggerSnapshotEntriesN,
		snapshotCatchUpEntriesN: cfg.SnapshotCatchUpEntriesN,
		stopc:                   make(chan struct{}),
		snapshotterReadyC:       make(chan *snap.Snapshotter, 1),
		notifyLeadership:        cfg.NotifyLeadership,
		// rest of structure populated after WAL replay
	}

	//re.transport.BindRaft(re.nodeID, re)
	//re.transport.BindDataNode(re.nodeID, cfg.DataNode)
	//re.transport.BindMetaNode(cfg.MetaNode)

	go re.startRaft()
	return RaftEngine{
		CommitC:           cc,
		ErrorC:            ec,
		SnapshotterReadyC: re.snapshotterReadyC,
		DoLead:            re.doLead,
		AdvanceC:          ac,
		RaftProcessor:     re.Process,
	}
}

func (re *raftEngine) doLead(old uint64) {
	re.node.TransferLeadership(context.TODO(), old, re.nodeID)
}

func (re *raftEngine) getWALDir() string {
	return path.Join(re.storageDir, "wal")
}

func (re *raftEngine) getSnapDir() string {
	return path.Join(re.storageDir, "snap")
}

func (re *raftEngine) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := re.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := re.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return re.wal.ReleaseLockTo(snap.Metadata.Index)
}

// entriesToApply returns not committed entries in ents.
func (re *raftEngine) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIndex := ents[0].Index
	if firstIndex > re.appliedIndex+1 {
		log.ZAPSugaredLogger().Errorf("Error raised when processing entries to apply, first index of committed entry [%d] should <= appliedIndex [%d].", firstIndex, re.appliedIndex)
		return
	}
	if re.appliedIndex-firstIndex+1 < uint64(len(ents)) {
		nents = ents[re.appliedIndex-firstIndex+1:]
	}
	return
}

// publishEntries will commit entries to state machine
func (re *raftEngine) publishEntries(ents []raftpb.Entry) bool {
	for _, ent := range ents {
		switch ent.Type {
		case raftpb.EntryNormal:
			if len(ent.Data) == 0 {
				// break switch, ignore empty messages
				break
			}
			select {
			case re.commitC <- &ent.Data:
				<-re.advanceC
			case <-re.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ent.Data)
			re.confState = *re.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				/// TODO : implement
				if len(cc.Context) > 0 {
					var ctx config.ConfChangeContext
					if err := pkg.Decode(cc.Context, &ctx); err != nil {
						log.ZAPSugaredLogger().Errorf("Error raised when decoding ConfChangeContext, err=%s.", err)
					}
					if err := re.transport.AddPod(ctx.PodID, ctx.URL); err != nil {
						log.ZAPSugaredLogger().Errorf("Error raised when adding pod, err=%s.", err)
					}
					if err := re.transport.AddNode(ctx.PodID, ctx.NodeID); err != nil {
						log.ZAPSugaredLogger().Errorf("Error raised when adding node, err=%s.", err)
					}
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(re.nodeID) {
					log.ZAPSugaredLogger().Infof("This node has been removed from the zone! Shutting down.")
					return false
				}
				//re.transport.RemoveNode(re.nodeID)
			}
		}

		//log.ZAPSugaredLogger().Debugf("Applied index [%d] -> [%d].", re.appliedIndex, ent.Index)
		re.appliedIndex = ent.Index
	}
	return true
}

func (re *raftEngine) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := re.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.ZAPSugaredLogger().Errorf("Error raised when loading snapshot, err=%s.", err)
	}
	return snapshot
}

func (re *raftEngine) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(re.getWALDir()) {
		if err := os.MkdirAll(re.getWALDir(), 0750); err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when creating dir for wal, err=%s.", err)
		}
		w, err := wal.Create(re.getWALDir(), nil)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when creating wal, err=%s.", err)
		}
		w.Close()
	}
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.ZAPSugaredLogger().Infof("Loading WAL at term [%d] and index [%d].", walsnap.Term, walsnap.Index)
	w, err := wal.Open(re.getWALDir(), walsnap)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when loading wal, dir=%s, err=%s.", re.getWALDir(), err)
	}
	return w
}

func (re *raftEngine) replayWAL() *wal.WAL {
	log.ZAPSugaredLogger().Infof("Replaying WAL of node %d.", re.nodeID)
	snapshot := re.loadSnapshot()
	w := re.openWAL(snapshot)
	_, state, ents, err := w.ReadAll()
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when reading WAL, err=%s.", err)
	}
	re.storage = raft.NewMemoryStorage()
	if snapshot != nil {
		re.storage.ApplySnapshot(*snapshot)
	}

	re.storage.SetHardState(state)
	re.storage.Append(ents)

	// send nil once lastIndex is published so client knows commit channel is current

	// trigger state machine load its snapshot
	if len(ents) > 0 {
		re.lastIndex = ents[len(ents)-1].Index
	}
	//else {
	//	re.lastIndex = snapshot.Metadata.Index // TODO : ???????????
	//}
	// TODO : ???????
	re.commitC <- nil
	log.ZAPSugaredLogger().Debugf("Trigger nil")

	return w
}

func (re *raftEngine) startRaft() {
	// create snap dir if not exists
	if !pkg.Exist(re.getSnapDir()) {
		if err := os.MkdirAll(re.getSnapDir(), 0750); err != nil {
			log.ZAPSugaredLogger().Fatalf("Error raised when creating dir for snapshot, err=%s.", err)
			panic(err)
		}
	}
	// create snapshotter
	re.snapshotter = snap.New(re.getSnapDir())
	re.snapshotterReadyC <- re.snapshotter

	oldWALExists := wal.Exist(re.getWALDir())
	re.wal = re.replayWAL()
	log.ZAPSugaredLogger().Debugf("finish replaying wal")

	c := &raft.Config{
		ID:              re.nodeID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         re.storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		PreVote:         true,
		CheckQuorum:     true,
	}
	if oldWALExists {
		re.node = raft.RestartNode(c)
	} else {
		peers := []raft.Peer{}
		if !re.join {
			for _, nodeID := range re.peers {
				peers = append(peers, raft.Peer{ID: nodeID})
			}
		}
		log.ZAPSugaredLogger().Debugf("peers : %+v", peers)
		re.node = raft.StartNode(c, peers)
	}

	go re.serveChannels()
}

func (re *raftEngine) stop() {
	close(re.commitC)
	close(re.errorC)
	re.transport.Stop()
	re.node.Stop()
}

func (re *raftEngine) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}
	log.ZAPSugaredLogger().Infof("Publishing snapshot at index [%d].", re.snapshotIndex)
	defer log.ZAPSugaredLogger().Infof("Finished publishing snapshot at index [%d].", re.snapshotIndex)
	if snapshotToSave.Metadata.Index <= re.appliedIndex {
		log.ZAPSugaredLogger().Errorf("Error raised when publishing snapshot, snapshot index [%d] should > appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, re.appliedIndex)
	}
	log.ZAPSugaredLogger().Debugf("Trigger nil")
	re.commitC <- nil // trigger to load snapshot
	re.confState = snapshotToSave.Metadata.ConfState
	re.snapshotIndex = snapshotToSave.Metadata.Index
	re.appliedIndex = snapshotToSave.Metadata.Index
}

func (re *raftEngine) maybeTriggerSnapshot() {
	if re.appliedIndex-re.snapshotIndex <= re.triggerSnapshotEntriesN {
		return
	}
	log.ZAPSugaredLogger().Infof("Start snapshot, appliedIndex [%d], last snapshot index [%d].", re.appliedIndex, re.snapshotIndex)
	data, err := re.getSnapshot()
	if err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when getting snapshot, err=%s.", err)
		panic(err)
	}
	snapshot, err := re.storage.CreateSnapshot(re.appliedIndex, &re.confState, data)
	if err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when creating snapshot, err=%s.", err)
		panic(err)
	}
	if err := re.saveSnap(snapshot); err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when saving snapshot, err=%s.", err)
		panic(err)
	}

	compactIndex := uint64(1)
	if re.appliedIndex > re.snapshotCatchUpEntriesN {
		compactIndex = re.appliedIndex - re.snapshotCatchUpEntriesN
	}
	if err := re.storage.Compact(compactIndex); err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when compacting snapshot, err=%s.", err)
		panic(err)
	}
	log.ZAPSugaredLogger().Infof("Compacted log at index [%d].", compactIndex)
	re.snapshotIndex = re.appliedIndex
}

func (re *raftEngine) serveChannels() {
	// get snapshot from storage
	snap, err := re.storage.Snapshot()
	if err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when getting snapshot from storage, err=%s.", err)
		panic(err)
	}
	// restore snapshot
	re.confState = snap.Metadata.ConfState
	re.snapshotIndex = snap.Metadata.Index
	re.appliedIndex = snap.Metadata.Index

	defer re.wal.Close()

	rand.Seed(time.Now().UnixNano())
	ticker := time.NewTicker(time.Millisecond * time.Duration(50+rand.Intn(100)))
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		var confChangeCount uint64 = 0
		for re.proposeC != nil && re.confChangeC != nil {
			select {
			case prop, ok := <-re.proposeC:
				if !ok {
					re.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					re.node.Propose(context.TODO(), []byte(prop))
				}
			case cc, ok := <-re.confChangeC:
				if !ok {
					re.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					re.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel, shutdown raft if not already
		close(re.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			re.node.Tick()
		case rd := <-re.node.Ready():
			// log.ZAPSugaredLogger().Infof("%+v", rd)
			re.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				log.ZAPSugaredLogger().Debugf("recv not empty snapshot !!!!!!!!!")
				re.saveSnap(rd.Snapshot)
				re.storage.ApplySnapshot(rd.Snapshot)
				re.publishSnapshot(rd.Snapshot)
			}
			re.storage.Append(rd.Entries)
			re.transport.Send(rd.Messages)
			if ok := re.publishEntries(re.entriesToApply(rd.CommittedEntries)); !ok {
				re.stop()
				return
			}
			// notify leadership if soft state is not nil
			if rd.SoftState != nil {
				if rd.SoftState.Lead == re.nodeID {
					re.notifyLeadership(re.nodeID)
				}
			}
			re.maybeTriggerSnapshot()
			re.node.Advance()

		case <-re.stopc:
			re.stop()
			return
			// TODO : errorC from transport
		}
	}
}

func (re *raftEngine) Process(ctx context.Context, m raftpb.Message) error {
	// if not ready, return nil
	if re.node == nil {
		return nil
	}
	return re.node.Step(ctx, m)
}

func (re *raftEngine) IsIDRemoved(id uint64) bool { return false }

func (re *raftEngine) ReportUnreachable(id uint64) {
	re.node.ReportUnreachable(id)
}

func (re *raftEngine) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	//re.node.ReportSnapshot(id, status)
}

func (re *raftEngine) LogEntries() {
	fi, _ := re.storage.FirstIndex()
	li, _ := re.storage.LastIndex()
	ents, err := re.storage.Entries(fi, li, uint64(100000))
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when getting entries : err=%s.", err)
		return
	}
	for _, ent := range ents {
		log.ZAPSugaredLogger().Debugf("entry term[%d] index[%d] type[%d].", ent.Term, ent.Index, ent.Type)
		if ent.Type == raftpb.EntryNormal {
			var cmd cmd2.MetaCMD
			err := pkg.Decode(ent.Data, &cmd)
			if err != nil {
				log.ZAPSugaredLogger().Errorf("Error raised when decoding cmd : err=%s.", err)
				continue
			}
			log.ZAPSugaredLogger().Debugf("%+v", cmd)
		}
	}

}
