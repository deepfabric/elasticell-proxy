package proxy

import (
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
)

var (
	errConnect            = errors.New("not connected")
	defaultConnectTimeout = time.Second * 5
)

func (p *RedisProxy) getConn(addr string) (goetty.IOSession, error) {
	conn := p.getConnLocked(addr)
	if p.checkConnect(addr, conn) {
		return conn, nil
	}

	return conn, errConnect
}

func (p *RedisProxy) getConnLocked(addr string) goetty.IOSession {
	p.RLock()
	conn := p.conns[addr]
	p.RUnlock()

	if conn != nil {
		return conn
	}

	return p.createConn(addr)
}

func (p *RedisProxy) createConn(addr string) goetty.IOSession {
	p.Lock()

	// double check
	if conn, ok := p.conns[addr]; ok {
		p.Unlock()
		return conn
	}

	conn := goetty.NewConnector(p.getConnectionCfg(addr), &redisDecoder{}, &redisEncoder{})
	p.conns[addr] = conn
	p.Unlock()
	return conn
}

func (p *RedisProxy) loopReadFromBackendServer(addr string, conn goetty.IOSession) {
	for {
		data, err := conn.Read()
		if err != nil {
			conn.Close()
			return
		}

		rsp, ok := data.(*raftcmdpb.Response)
		if ok {
			log.Debugf("backend-[%s]: read a response: uuid=<%+v> resp=<%+v>",
				addr,
				rsp.UUID,
				rsp)
			p.onResp(rsp)
		}
	}
}

func (p *RedisProxy) getConnectionCfg(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr: addr,
		TimeoutConnectToServer: defaultConnectTimeout,
	}
}

func (p *RedisProxy) checkConnect(addr string, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Errorf("transport: connect to store failure, target=<%s> errors:\n %+v",
			addr,
			err)
		return false
	}

	go p.loopReadFromBackendServer(addr, conn)
	return ok
}
