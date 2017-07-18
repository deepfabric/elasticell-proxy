package proxy

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
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

const (
	defaultChanSize = 1024
)

var (
	pingReq = &raftcmdpb.Request{
		Cmd: [][]byte{[]byte("ping")},
	}
)

type req struct {
	rs      *redisSession
	raftReq *raftcmdpb.Request
}

// RedisProxy is a redis proxy
type RedisProxy struct {
	sync.RWMutex

	cfg             *Cfg
	svr             *goetty.Server
	pdClient        *pd.Client
	supportCmds     map[string]struct{}
	ranges          *util.CellTree
	cellLeaderAddrs map[uint64]string           // cellid -> leader peer store addr
	conns           map[string]goetty.IOSession // store addr -> netconn
	routing         *routing                    // uuid -> session
	syncEpoch       uint64
	reqs            chan *req
	pings           chan string

	cancel   context.CancelFunc
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
		reqs:            make(chan *req, defaultChanSize),
		pings:           make(chan string, defaultChanSize),
	}
}

// Start starts the proxy
func (p *RedisProxy) Start() error {
	go p.listenToStop()

	p.init()

	ctx, cancel := context.WithCancel(context.TODO())
	go p.readyToHandleRedisCommand(ctx)
	p.cancel = cancel

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
		for addr, conn := range p.conns {
			conn.Close()
			log.Infof("stop: store connection closed, addr=<%s>", addr)
		}

		p.svr.Stop()
		log.Infof("stop: tcp listen stopped")

		p.cancel()
	})
}

func (p *RedisProxy) init() {
	p.refreshRanges()

	for _, cmd := range p.cfg.SupportCMDs {
		p.supportCmds[cmd] = struct{}{}
	}
}

func (p *RedisProxy) getSyncEpoch() uint64 {
	return atomic.LoadUint64(&p.syncEpoch)
}

func (p *RedisProxy) refreshRanges() {
	old := p.getSyncEpoch()
	log.Infof("pd-sync: try to sync, epoch=<%d>", old)

	p.Lock()
	if old < p.syncEpoch {
		log.Infof("pd-sync: already sync, skip, old=<%d> new=<%d>", old, p.syncEpoch)
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
		r, err := session.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			log.Errorf("redis-[%s]: read from cli failed, errors\n %+v",
				addr,
				err)
			return err
		}

		cmd := r.(redis.Command)
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

		p.addToForward(rs, raftReq)
	}
}

func (p *RedisProxy) addToPing(target string) {
	p.pings <- target
}

func (p *RedisProxy) addToForward(rs *redisSession, raftReq *raftcmdpb.Request) {
	p.reqs <- &req{
		rs:      rs,
		raftReq: raftReq,
	}
}

func (p *RedisProxy) readyToHandleRedisCommand(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(p.reqs)
			close(p.pings)
			log.Infof("stop: handle redis command stopped")
			return
		case req := <-p.reqs:
			if req != nil {
				p.handleReq(req.rs, req.raftReq)
			}
		case target := <-p.pings:
			if target != "" {
				p.forwardTo(target, pingReq, nil)
			}
		}
	}
}

func (p *RedisProxy) handleReq(rs *redisSession, req *raftcmdpb.Request) {
	var err error
	for {
		if rs.lastRetries >= p.cfg.MaxRetries {
			rs.lastRetries = 0
			break
		}

		target := ""

		if len(req.Cmd) <= 1 {
			target = p.getRandomStoreAddr()
		} else {
			target = p.getLeaderStoreAddr(req.Cmd[1])
		}

		if log.DebugEnabled() {
			log.Debugf("redis-[%s]: handle req, leader=<%s> times=<%d>",
				rs.session.RemoteAddr(),
				target,
				rs.lastRetries)
		}

		if target == "" {
			err = fmt.Errorf("leader not found for key: %v", req.Cmd[1])
		} else {
			err = p.forwardTo(target, req, rs)
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

	if nil != rs {
		if log.DebugEnabled() {
			log.Debugf("redis-[%s]: write to, to=<%s> epoch=<%d> uuid=<%v>",
				rs.session.RemoteAddr(),
				addr,
				req.Epoch,
				req.UUID)
		}
		p.routing.put(req.UUID, rs)
	}

	return nil
}

func (p *RedisProxy) onResp(rsp *raftcmdpb.Response) {
	rs := p.routing.delete(rsp.UUID)
	if rs != nil && !rs.isClosed() {
		if rsp.Type == raftcmdpb.RaftError {
			rs.addToRetry(rsp.OriginRequest)
			return
		}

		rs.onResp(rsp)
	} else if len(rsp.UUID) > 0 {
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

func (p *RedisProxy) getRandomStoreAddr() string {
	p.RLock()
	var target *metapb.Cell
	p.ranges.Ascend(func(cell *metapb.Cell) bool {
		target = cell
		return false
	})
	addr := p.cellLeaderAddrs[target.ID]
	p.RUnlock()

	return addr
}
