package proxy

import (
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
)

var (
	errConnect            = errors.New("not connected")
	defaultConnectTimeout = time.Second * 5
)

type backend struct {
	sync.RWMutex

	p    *RedisProxy
	addr string
	conn goetty.IOSession
	reqs *util.Queue
}

func newBackend(p *RedisProxy, addr string, conn goetty.IOSession) *backend {
	return &backend{
		p:    p,
		addr: addr,
		conn: conn,
		reqs: &util.Queue{},
	}
}

func (bc *backend) isConnected() bool {
	bc.RLock()
	v := bc.conn.IsConnected()
	bc.RUnlock()

	return v
}

func (bc *backend) connect() (bool, error) {
	bc.Lock()
	yes, err := bc.conn.Connect()
	if yes {
		bc.reqs = &util.Queue{}
	}
	bc.Unlock()

	return yes, err
}

func (bc *backend) close() {
	bc.Lock()
	if bc.conn != nil {
		bc.conn.Close()
	}

	reqs := bc.reqs.Dispose()
	for _, v := range reqs {
		r := v.(*req)
		r.errorDone(errConnect)
	}
	bc.Unlock()
}

func (bc *backend) addReq(r *req) error {
	bc.Lock()

	if !bc.conn.IsConnected() {
		bc.Unlock()
		return errConnect
	}

	err := bc.reqs.Put(r)

	bc.Unlock()

	return err
}

func (bc *backend) readLoop() {
	for {
		data, err := bc.conn.ReadTimeout(time.Second * 10)
		if err != nil {
			log.Errorf("backend-[%s]: read error: %s",
				bc.addr,
				err)
			bc.close()
			return
		}

		rsp, ok := data.(*raftcmdpb.Response)
		if ok && len(rsp.UUID) > 0 {
			log.Debugf("backend-[%s]: read a response: uuid=<%+v> resp=<%+v>",
				bc.addr,
				rsp.UUID,
				rsp)

			bc.p.onResp(rsp)
		}
	}
}

func (bc *backend) writeLoop() {
	items := make([]interface{}, batch, batch)

	for {
		n, err := bc.reqs.Get(batch, items)
		if err != nil {
			log.Errorf("backend-[%s]: exit write loop",
				bc.addr)
			return
		}

		out := bc.conn.OutBuf()
		for i := int64(0); i < n; i++ {
			r := items[i].(*req)
			writeRaftRequest(r.raftReq, out)
			if log.DebugEnabled() && len(r.raftReq.UUID) > 0 {
				log.Debugf("backend-[%s]: write req epoch=<%d> uuid=<%v>",
					bc.addr,
					r.raftReq.Epoch,
					r.raftReq.UUID)
			}
		}
		err = bc.conn.WriteOutBuf()
		if err != nil {
			for i := int64(0); i < n; i++ {
				r := items[i].(*req)
				r.errorDone(err)
			}
		}
	}
}

func (p *RedisProxy) getConn(addr string) (*backend, error) {
	bc := p.getConnLocked(addr)
	if p.checkConnect(addr, bc) {
		return bc, nil
	}

	return bc, errConnect
}

func (p *RedisProxy) getConnLocked(addr string) *backend {
	p.RLock()
	bc := p.bcs[addr]
	p.RUnlock()

	if bc != nil {
		return bc
	}

	return p.createConn(addr)
}

func (p *RedisProxy) createConn(addr string) *backend {
	p.Lock()

	// double check
	if bc, ok := p.bcs[addr]; ok {
		p.Unlock()
		return bc
	}

	conn := goetty.NewConnector(p.getConnectionCfg(addr), &redisDecoder{}, &redisEncoder{})
	b := newBackend(p, addr, conn)
	p.bcs[addr] = b
	p.Unlock()
	return b
}

func (p *RedisProxy) sendHeartbeat(addr string, session goetty.IOSession) {
	p.addToPing(addr)
}

func (p *RedisProxy) getConnectionCfg(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr: addr,
		TimeoutConnectToServer: defaultConnectTimeout,
		TimeoutWrite:           time.Second,
		WriteTimeoutFn:         p.sendHeartbeat,
		TimeWheel:              goetty.NewTimeoutWheel(goetty.WithTickInterval(time.Millisecond * 500)),
	}
}

func (p *RedisProxy) checkConnect(addr string, bc *backend) bool {
	if nil == bc {
		return false
	}

	p.Lock()
	if bc.isConnected() {
		p.Unlock()
		return true
	}

	ok, err := bc.connect()
	if err != nil {
		log.Errorf("transport: connect to store failure, target=<%s> errors:\n %+v",
			addr,
			err)
		p.Unlock()
		return false
	}

	go bc.readLoop()
	go bc.writeLoop()
	p.Unlock()
	return ok
}
