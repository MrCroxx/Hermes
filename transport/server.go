package transport

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/fwhezfwhez/tcpx"
	"mrcroxx.io/hermes/cmd"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/unit"
	"sync"
)

const (
	RaftID              = 1
	HermesProducerCMDID = 2
	HermesProducerRSPID = 3
	HermesConsumerCMDID = 4
	HermesConsumerRSPID = 5
)

var (
	errRedirect = errors.New("NodeID not exists in this pod, redirect")
	REDIRECT    = "redirect"
	INDEX       = "index"
	UNDIRECTED  = "undirected"
	LOSTINDEX   = " lost index"
)

type tcpServer struct {
	url    string
	core   unit.Core
	server *tcpx.TcpX
	mux    sync.Mutex
}

func NewTCPServer(url string, core unit.Core) TCPServer {
	return &tcpServer{
		url:  url,
		core: core,
	}
}

func (s *tcpServer) Init() error {
	s.server = tcpx.NewTcpX(tcpx.ProtobufMarshaller{})
	s.server.OnConnect = s.onConnect
	s.server.OnClose = s.onClose
	s.server.AddHandler(RaftID, s.handleRaft)
	s.server.AddHandler(HermesProducerCMDID, s.handleHermesProducer)
	go func() {
		err := s.server.ListenAndServe("tcp", s.url)
		if err != nil {
			log.ZAPSugaredLogger().Fatalf("Error raised when starting listen and serve, err=%s.", err)
			panic(err)
		}
	}()
	return nil
}

func (s *tcpServer) Close() {
	s.server.Stop(true)
}

func (s *tcpServer) onConnect(c *tcpx.Context) {

	log.ZAPSugaredLogger().Debugf("got a new conn : %s .", c.ClientIP())
}

func (s *tcpServer) onClose(c *tcpx.Context) {
	log.ZAPSugaredLogger().Debugf("tcp conn closed. %+v", c)
}

func (s *tcpServer) handleRaft(c *tcpx.Context) {
	var m raftpb.Message
	_, err := c.BindWithMarshaller(&m, tcpx.JsonMarshaller{})
	//log.ZAPSugaredLogger().Debugf("%+v", m)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when binding message, err=%s.", err)
		return
	}
	rp := s.core.RaftProcessor(m.To)
	if rp == nil {
		log.ZAPSugaredLogger().Infof("Not a message to this pod or not finish initializing yet, nodeID=%d.", m.To)
		return
	}
	err = rp(context.TODO(), m)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when processing raft message, nodeID=%d, err=%s.", m.To, err)
	}
}

func (s *tcpServer) handleHermesProducer(c *tcpx.Context) {
	var req cmd.HermesProducerCMD
	var rsp cmd.HermesProducerRSP

	// bind rsp
	_, err := c.BindWithMarshaller(&req, tcpx.JsonMarshaller{})
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when binding message, err=%s.", err)
		rsp.Err = fmt.Sprintf("%s", err)
		err = c.ReplyWithMarshaller(tcpx.JsonMarshaller{}, HermesProducerRSPID, rsp)
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when replying client, err=%s.", err)
		}
		return
	}

	// lookup zone leader
	var leaderID uint64
	leaderID, rsp.PodID = s.core.LookUpLeader(req.ZoneID)
	// if zone leader not in this pod, redirect
	if rsp.PodID != s.core.PodID() {
		rsp.Err = REDIRECT
		s.reply(c, &rsp)
		return
	}
	if req.Index == 0 {
		rsp.Index = s.core.LookUpNextFreshIndex(leaderID)
		rsp.Err = INDEX
		s.reply(c, &rsp)
		return
	}
	nIndex := s.core.LookUpNextFreshIndex(leaderID)
	if nIndex == 0 {
		rsp.Err = UNDIRECTED
		s.reply(c, &rsp)
		return
	}
	offset := nIndex - req.Index
	if offset < 0 {
		rsp.Err = LOSTINDEX
		rsp.Index = nIndex
		s.reply(c, &rsp)
	}
	if int(offset) >= len(req.Data) {
		rsp.TS = req.TS
		s.reply(c, &rsp)
		return
	}
	if !s.core.AppendData(leaderID, req.TS, req.Data[offset:], func(ts int64) {
		rsp.TS = ts
		rsp.Index = nIndex + uint64(len(req.Data[offset:]))
		s.reply(c, &rsp)
		return
	}) {
		rsp.Err = UNDIRECTED
		s.reply(c, &rsp)
		return
	}
}

func (s *tcpServer) reply(c *tcpx.Context, rsp *cmd.HermesProducerRSP) {
	err := c.ReplyWithMarshaller(tcpx.JsonMarshaller{}, HermesProducerRSPID, *rsp)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised when replying client, err=%s.", err)
	}
}
