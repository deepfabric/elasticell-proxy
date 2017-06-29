package proxy

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// RedisProxy is a redis proxy
type RedisProxy struct {
	sync.RWMutex

	cfg             *Cfg
	svr             *goetty.Server
	pdClient        *pd.Client
	supportCmds     map[string]struct{} // TODO: init
	ranges          *util.CellTree
	cellLeaderAddrs map[uint64]string           // cellid -> leader peer store addr
	conns           map[string]goetty.IOSession // store addr -> netconn
	routing         *routing                    // uuid -> session
	syncEpoch       uint64

	stopOnce sync.Once
	stopWG   sync.WaitGroup
	stopC    chan struct{}
}

// NewRedisProxy returns a redisp proxy
func NewRedisProxy(cfg *Cfg) *RedisProxy {
	client, err := pd.NewClient(fmt.Sprintf("proxy-%s", cfg.Addr), cfg.PDAddrs...)
	if err != nil {
		log.Fatalf("bootstrap: create pd client failed, errors:\n%+v", err)
	}

	tcpSvr := goetty.NewServer(cfg.Addr,
		redis.NewRedisDecoder(),
		goetty.NewEmptyEncoder(),
		goetty.NewInt64IDGenerator())

	return &RedisProxy{
		pdClient:        client,
		cfg:             cfg,
		svr:             tcpSvr,
		supportCmds:     make(map[string]struct{}),
		routing:         newRouting(),
		ranges:          util.NewCellTree(),
		cellLeaderAddrs: make(map[uint64]string),
		conns:           make(map[string]goetty.IOSession),
		stopC:           make(chan struct{}),
	}
}

// Start starts the proxy
func (p *RedisProxy) Start() error {
	go p.listenToStop()

	p.init()
	return p.svr.Start(p.doConnection)
}

// Stop stop the proxy
func (p *RedisProxy) Stop() {
	p.stopWG.Add(1)
	p.stopC <- struct{}{}
	p.stopWG.Wait()
}

func (p *RedisProxy) listenToStop() {
	<-p.stopC
	p.doStop()
}

func (p *RedisProxy) doStop() {
	p.stopOnce.Do(func() {
		defer p.stopWG.Done()

		log.Infof("stop: start to stop redis proxy")

		p.Lock()
		for addr, conn := range p.conns {
			conn.Close()
			log.Infof("stop: store connection closed, addr=<%s>", addr)
		}
		p.Unlock()

		p.svr.Stop()
		log.Infof("stop: tcp listen stopped")
	})
}

func (p *RedisProxy) init() {
	p.refreshRanges()

	for _, cmd := range p.cfg.SupportCMDs {
		p.supportCmds[cmd] = struct{}{}
	}
}

func (p *RedisProxy) getSyncEpoch() uint64 {
	p.RLock()
	v := p.syncEpoch
	p.RUnlock()

	return v
}

func (p *RedisProxy) refreshRanges() {
	old := p.getSyncEpoch()

	p.Lock()
	if old < p.syncEpoch {
		p.Unlock()
		return
	}

	rsp, err := p.pdClient.GetLastRanges(context.TODO(), &pdpb.GetLastRangesReq{})
	if err != nil {
		log.Fatalf("bootstrap: init cell ranges failed, errors:\n%+v", err)
	}

	p.clean()
	for _, r := range rsp.Ranges {
		p.ranges.Update(r.Cell)
		p.cellLeaderAddrs[r.Cell.ID] = r.LeaderStore.ClientAddress
	}
	p.syncEpoch++

	log.Infof("pd-sync: sync complete, epoch=%d", p.syncEpoch)
	p.Unlock()
}

func (p *RedisProxy) clean() {
	for id := range p.cellLeaderAddrs {
		delete(p.cellLeaderAddrs, id)
	}
	p.ranges = util.NewCellTree()
}

func (p *RedisProxy) doConnection(session goetty.IOSession) error {
	addr := session.RemoteAddr()
	log.Infof("redis-[%s]: connected", addr)

	// every client has 3 goroutines, read,write,retry
	rs := newSession(session)
	go rs.writeLoop()
	go rs.retryLoop(p)
	defer rs.close()

	for {
		req, err := session.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			log.Errorf("redis-[%s]: read from cli failed, errors\n %+v",
				addr,
				err)
			return err
		}

		cmd := req.(redis.Command)
		if log.DebugEnabled() {
			log.Debugf("redis-[%s]: read a cmd: %s", addr, cmd.ToString())
		}

		_, ok := p.supportCmds[cmd.CmdString()]
		if !ok {
			rs.respError(fmt.Errorf("redis command not support: %s", cmd.CmdString()))
			continue
		}

		raftReq := &raftcmdpb.Request{
			UUID:  uuid.NewV4().Bytes(),
			Cmd:   cmd,
			Epoch: p.getSyncEpoch(),
		}

		rs.lastRetries = 0
		p.handleReq(rs, raftReq)
	}
}

func (p *RedisProxy) handleReq(rs *redisSession, req *raftcmdpb.Request) {
	var err error

	for {
		if rs.lastRetries >= p.cfg.MaxRetries {
			rs.lastRetries = 0
			break
		}

		leader := p.getLeaderStoreAddr(req.Cmd[1])
		if log.DebugEnabled() {
			log.Debugf("redis-[%s]: handle req, leader=<%s> times=<%d>",
				rs.session.RemoteAddr(),
				leader,
				rs.lastRetries)
		}

		if leader == "" {
			err = fmt.Errorf("leader not found for key: %v", req.Cmd[1])
		} else {
			err = p.forwardTo(leader, req, rs)
			if err == nil {
				rs.lastRetries++
				break
			}
		}

		time.Sleep(time.Duration(p.cfg.RetryDuration) * time.Millisecond)
		// update last range
		p.refreshRanges()

		rs.lastRetries++
	}

	if err != nil {
		log.Errorf("redis-[%s]: handle returns errors: %+v",
			rs.session.RemoteAddr(),
			err)

		rs.lastRetries = 0
		p.routing.delete(req.UUID)
		rs.respError(err)
	}
}

func (p *RedisProxy) forwardTo(addr string, req *raftcmdpb.Request, rs *redisSession) error {
	conn, err := p.getConn(addr)
	if err != nil {
		return errors.Wrapf(err, "getConn")
	}

	req.Epoch = p.getSyncEpoch()
	err = conn.Write(req)
	if err != nil {
		conn.Close()
		return errors.Wrapf(err, "writeTo")
	}

	if log.DebugEnabled() {
		log.Debugf("redis-[%s]: write to, to=<%s> epoch=<%d> uuid=<%v>",
			rs.session.RemoteAddr(),
			addr,
			req.Epoch,
			req.UUID)
	}

	p.routing.put(req.UUID, rs)
	return nil
}

func (p *RedisProxy) onResp(rsp *raftcmdpb.Response) {
	rs := p.routing.delete(rsp.UUID)
	if rs != nil && !rs.isClosed() {
		if rsp.Type == raftcmdpb.RaftError {
			// we need sync cell,store and leader info from pd server, than retry this request
			rs.addToRetry(rsp.OriginRequest)
			return
		}

		rs.onResp(rsp)
	} else {
		log.Warnf("redis-resp: client maybe closed, ingore resp, uuid=<%+v>",
			rsp.UUID)
	}
}

func (p *RedisProxy) getLeaderStoreAddr(key []byte) string {
	p.RLock()
	cell := p.ranges.Search(key)
	addr := p.cellLeaderAddrs[cell.ID]
	p.RUnlock()

	return addr
}
