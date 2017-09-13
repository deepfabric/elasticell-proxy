package proxy

import (
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/deepfabric/discovery"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc/naming"
)

const (
	batch = 64
)

var (
	pingReq = &raftcmdpb.Request{
		Cmd: [][]byte{[]byte("ping")},
	}
)

type req struct {
	rs      *redisSession
	raftReq *raftcmdpb.Request
	retries int
}

func (r *req) errorDone(err error) {
	if r.rs != nil && !r.rs.isClosed() {
		r.rs.errorResp(err)
	}
}

func (r *req) done(rsp *raftcmdpb.Response) {
	if r.rs != nil && !r.rs.isClosed() {
		r.rs.resp(rsp)
	}
}

// RedisProxy is a redis proxy
type RedisProxy struct {
	sync.RWMutex

	cfg             *Cfg
	svr             *goetty.Server
	pdClient        *pd.Client
	supportCmds     map[string]struct{}
	ranges          *util.CellTree
	cellLeaderAddrs map[uint64]string   // cellid -> leader peer store addr
	bcs             map[string]*backend // store addr -> netconn
	routing         *routing            // uuid -> session
	syncEpoch       uint64
	reqs            []*queue.Queue
	retries         *queue.Queue
	pings           chan string
	watcher         *discovery.EtcdWatcher

	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopWG   sync.WaitGroup
	stopC    chan struct{}
}

// NewRedisProxy returns a redisp proxy
func NewRedisProxy(cfg *Cfg) *RedisProxy {
	var err error
	var client *pd.Client
	client, err = pd.NewClient(fmt.Sprintf("proxy-%s", cfg.Addr), cfg.PDAddrs...)
	if err != nil {
		log.Fatalf("bootstrap: create pd client failed, errors:\n%+v", err)
	}

	tcpSvr := goetty.NewServer(cfg.Addr,
		redis.NewRedisDecoder(),
		goetty.NewEmptyEncoder(),
		goetty.NewInt64IDGenerator())

	p := &RedisProxy{
		pdClient:        client,
		cfg:             cfg,
		svr:             tcpSvr,
		supportCmds:     make(map[string]struct{}),
		routing:         newRouting(),
		ranges:          util.NewCellTree(),
		cellLeaderAddrs: make(map[uint64]string),
		bcs:             make(map[string]*backend),
		stopC:           make(chan struct{}),
		reqs:            make([]*queue.Queue, cfg.WorkerCount),
		retries:         &queue.Queue{},
		pings:           make(chan string),
	}

	p.watcher, err = discovery.NewEtcdWatcher("/elasticell", "query", cfg.PDAddrs)
	if err != nil {
		log.Fatalf("bootstrap: create etcd watcher failed, errors:\n%+v", err)
	}

	for index := 0; index < cfg.WorkerCount; index++ {
		p.reqs[index] = &queue.Queue{}
	}

	p.init()

	return p
}

// Start starts the proxy
func (p *RedisProxy) Start() error {
	go p.listenToStop()
	go p.readyToHandleReq(p.ctx)

	return p.svr.Start(p.doConnection)
}

func (p *RedisProxy) watchLoop() {
	var err error
	var updates []*naming.Update
	for {
		if updates, err = p.watcher.Next(); err != nil {
			log.Infof("watchLoop quited due to error:\n%+v", err)
			return
		}
		if len(updates) == 0 {
			continue
		}
		log.Infof("watchLoop updates: %# v", pretty.Formatter(updates))
		for _, u := range updates {
			switch u.Op {
			case naming.Add:
				_ = p.getConnLocked(u.Addr)
			case naming.Delete:
				p.closeConn(u.Addr)
			default:
				log.Fatalf("watchLoop unrecognized update op: %v", u.Op)
			}
		}
	}
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
		for _, bc := range p.bcs {
			bc.close()
			log.Infof("stop: store connection closed, addr=<%s>", bc.addr)
		}

		p.svr.Stop()
		log.Infof("stop: tcp listen stopped")

		p.cancel()
	})
}

func (p *RedisProxy) init() {
	p.ctx, p.cancel = context.WithCancel(context.TODO())
	for _, cmd := range p.cfg.SupportCMDs {
		p.supportCmds[cmd] = struct{}{}
	}

	p.refreshRanges(true)

}

func (p *RedisProxy) getSyncEpoch() uint64 {
	return atomic.LoadUint64(&p.syncEpoch)
}

func (p *RedisProxy) refreshRanges(immediate bool) {
	old := p.getSyncEpoch()
	log.Infof("pd-sync: try to sync, epoch=<%d>", old)

	p.Lock()
	if old < p.syncEpoch {
		log.Infof("pd-sync: already sync, skip, old=<%d> new=<%d>", old, p.syncEpoch)
		p.Unlock()
		return
	}

	if !immediate {
		time.Sleep(time.Duration(p.cfg.RetryDuration) * time.Millisecond)
	}

	rsp, err := p.pdClient.GetLastRanges(context.TODO(), &pdpb.GetLastRangesReq{})
	if err != nil {
		log.Fatalf("bootstrap: get cell ranges from pd failed, errors:\n%+v", err)
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

	// every client has 2 goroutines, read, write
	rs := newSession(session)
	go rs.writeLoop()
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
			rs.errorResp(fmt.Errorf("command not support: %s", cmd.CmdString()))
			continue
		}

		p.addToForward(&req{
			raftReq: &raftcmdpb.Request{
				UUID: uuid.NewV4().Bytes(),
				Cmd:  cmd,
			},
			rs:      rs,
			retries: 0,
		})
	}
}

func (p *RedisProxy) addToPing(target string) {
	p.pings <- target
}

func (p *RedisProxy) retry(r *req) {
	r.retries++
	p.retries.Put(r)
}

func (p *RedisProxy) addToForward(r *req) {
	if r.raftReq == nil {
		log.Fatalf("bug: raft req cannot be nil")
	}

	if r.retries == 0 {
		r.raftReq.Epoch = p.getSyncEpoch()
	}

	if len(r.raftReq.Cmd) <= 1 {
		p.reqs[0].Put(r)
		return
	}

	key := int(crc32.ChecksumIEEE(r.raftReq.Cmd[1]))
	index := (p.cfg.WorkerCount - 1) & key
	p.reqs[index].Put(r)
}

func (p *RedisProxy) readyToHandleReq(ctx context.Context) {
	for _, q := range p.reqs {
		go func(q *queue.Queue) {
			log.Infof("bootstrap: handle redis command started")

			for {
				reqs, err := q.Get(batch)
				if nil != err {
					log.Infof("stop: handle redis command stopped")
					return
				}

				for _, v := range reqs {
					r := v.(*req)
					p.handleReq(r)
				}
			}
		}(q)
	}

	go func() {
		log.Infof("bootstrap: handle redis retries command started")

		for {
			reqs, err := p.retries.Get(batch)
			if nil != err {
				log.Infof("stop: handle redis retries command stopped")
				return
			}

			for _, v := range reqs {
				r := v.(*req)
				p.handleReq(r)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			for _, q := range p.reqs {
				q.Dispose()
			}

			close(p.pings)
			log.Infof("stop: handle redis command stopped")
			return
		case target := <-p.pings:
			if target != "" {
				p.forwardTo(target, &req{
					raftReq: pingReq,
				})
			}
		}
	}
}

func (p *RedisProxy) handleReq(r *req) {
	if r.retries >= p.cfg.MaxRetries {
		err := fmt.Errorf("retry %d times failed", r.retries)
		log.Errorf("redis-[%s]: handle error: uuid=<%+v>, error=<%s>",
			r.rs.addr,
			r.raftReq.UUID,
			err)
		p.routing.delete(r.raftReq.UUID)
		r.errorDone(err)
		return
	} else if r.retries > 0 {
		if r.raftReq.Epoch >= p.getSyncEpoch() {
			p.refreshRanges(false)
		}
	}

	target := ""
	var cellID uint64

	if len(r.raftReq.Cmd) <= 1 {
		target = p.getRandomStoreAddr()
	} else if strings.ToLower(string(r.raftReq.Cmd[0])) == "query" {

	} else {
		target, cellID = p.getLeaderStoreAddr(r.raftReq.Cmd[1])
	}

	if log.DebugEnabled() {
		log.Debugf("req: handle req, uuid=<%+v>, cell=<%d>, bc=<%s> times=<%d> ",
			r.raftReq.UUID,
			cellID,
			target,
			r.retries)
	}

	if target == "" {
		log.Errorf("req: leader not found for key, uuid=<%+v> key=<%v>",
			r.raftReq.UUID,
			r.raftReq.Cmd[1])
		p.retry(r)
		return
	}

	err := p.forwardTo(target, r)
	if err != nil {
		log.Errorf("req: forward failed, uuid=<%+v> error=<%s>",
			r.raftReq.UUID,
			err)
		p.retry(r)
		return
	}
}

func (p *RedisProxy) forwardTo(addr string, r *req) error {
	bc, err := p.getConn(addr)
	if err != nil {
		return errors.Wrapf(err, "getConn")
	}

	r.raftReq.Epoch = p.getSyncEpoch()

	if nil != r.rs {
		p.routing.put(r.raftReq.UUID, r)
	}

	err = bc.addReq(r)
	if err != nil {
		p.routing.delete(r.raftReq.UUID)
		return errors.Wrapf(err, "writeTo")
	}

	if nil != r.raftReq && len(r.raftReq.UUID) > 0 {
		log.Debugf("req: added to backend queue, uuid=<%+v>", r.raftReq.UUID)
	}
	return nil
}

func (p *RedisProxy) onResp(rsp *raftcmdpb.Response) {
	r := p.routing.delete(rsp.UUID)
	if r != nil {
		if rsp.Type == raftcmdpb.RaftError {
			p.retry(r)
			return
		}

		r.done(rsp)
	} else if len(rsp.UUID) > 0 {
		log.Debugf("redis-resp: client maybe closed, ingore resp, uuid=<%+v>",
			rsp.UUID)
	}
}

func (p *RedisProxy) getLeaderStoreAddr(key []byte) (string, uint64) {
	p.RLock()
	cell := p.ranges.Search(key)
	addr := p.cellLeaderAddrs[cell.ID]
	p.RUnlock()

	return addr, cell.ID
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
