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
	cellLeaderAddrs map[uint64]string            // cellid -> leader peer store addr
	conns           map[string]*goetty.Connector // store addr -> netconn
	routing         *routing                     // uuid -> session
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

	return &RedisProxy{
		svr: goetty.NewServer(cfg.Addr,
			redis.NewRedisDecoder(),
			goetty.NewEmptyEncoder(),
			goetty.NewInt64IDGenerator()),
		pdClient: client,

		ranges:          util.NewCellTree(),
		cellLeaderAddrs: make(map[uint64]string),
		conns:           make(map[string]*goetty.Connector),

		stopC: make(chan struct{}),
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
		p.cellLeaderAddrs[r.Cell.ID] = r.LeaderStore.Address
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
	log.Debugf("redis-[%s]: connected", addr)

	// every client has 2 goroutines, read and write
	rs := newSession(session)
	go rs.writeLoop()
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

		p.handleReq(rs, raftReq)
	}
}

func (p *RedisProxy) handleReq(rs *redisSession, req *raftcmdpb.Request) {
	var err error
	retries := 0
	for {
		if retries >= p.cfg.MaxRetries {
			break
		}

		leader := p.getLeaderStoreAddr(req.Cmd[1])
		if leader == "" {
			err = fmt.Errorf("leader not found for key: %v", req.Cmd[1])
		} else {
			err = p.forwardTo(leader, req, rs)
			if err == nil {
				break
			}
		}

		time.Sleep(time.Duration(p.cfg.RetryDuration) * time.Millisecond)
		// update last range
		p.refreshRanges()

		retries++
	}

	if err != nil {
		p.routing.delete(req.UUID)

		log.Errorf("redis-[%s]: handle returns errors: %+v",
			rs.session.RemoteAddr(),
			err)
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

	p.routing.put(req.UUID, rs)
	return nil
}

func (p *RedisProxy) onResp(rsp *raftcmdpb.Response) {
	rs := p.routing.delete(rsp.UUID)
	if rs != nil {
		if rsp.Type == raftcmdpb.RaftError {
			// we need sync cell,store and leader info from pd server, than retry this request
			rs.addToRetry(rsp.OriginRequest)
			return
		}

		rs.onResp(rsp)
	}
}

func (p *RedisProxy) getLeaderStoreAddr(key []byte) string {
	p.RLock()
	cell := p.ranges.Search(key)
	addr := p.cellLeaderAddrs[cell.ID]
	p.RUnlock()

	return addr
}
