package proxy

import (
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	credis "github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

type redisSession struct {
	sync.RWMutex

	session goetty.IOSession
	resps   *util.Queue
	addr    string
}

func newSession(session goetty.IOSession) *redisSession {
	return &redisSession{
		session: session,
		resps:   &util.Queue{},
		addr:    session.RemoteAddr(),
	}
}

func (rs *redisSession) close() {
	rs.Lock()
	rs.resps.Dispose()
	log.Infof("redis-[%s]: closed", rs.addr)
	rs.Unlock()
}

func (rs *redisSession) resp(rsp *raftcmdpb.Response) {
	rs.resps.Put(rsp)
}

func (rs *redisSession) errorResp(err error) {
	rs.resp(&raftcmdpb.Response{
		ErrorResult: util.StringToSlice(err.Error()),
	})
}

func (rs *redisSession) writeLoop() {
	items := make([]interface{}, batch, batch)

	for {
		rs.RLock()
		n, err := rs.resps.Get(batch, items)
		if nil != err {
			rs.RUnlock()
			return
		}

		buf := rs.session.OutBuf()
		for i := int64(0); i < n; i++ {
			rs.doResp(items[i].(*raftcmdpb.Response), buf)
		}
		rs.session.WriteOutBuf()
		rs.RUnlock()
	}
}

func (rs *redisSession) doResp(resp *raftcmdpb.Response, buf *goetty.ByteBuf) {
	if resp.ErrorResult != nil {
		redis.WriteError(resp.ErrorResult, buf)
	}

	if resp.ErrorResults != nil {
		for _, err := range resp.ErrorResults {
			redis.WriteError(err, buf)
		}
	}

	if resp.BulkResult != nil || resp.HasEmptyBulkResult != nil {
		redis.WriteBulk(resp.BulkResult, buf)
	}

	if resp.FvPairArrayResult != nil || resp.HasEmptyFVPairArrayResult != nil {
		credis.WriteFVPairArray(resp.FvPairArrayResult, buf)
	}

	if resp.IntegerResult != nil {
		redis.WriteInteger(*resp.IntegerResult, buf)
	}

	if resp.ScorePairArrayResult != nil || resp.HasEmptyScorePairArrayResult != nil {
		credis.WriteScorePairArray(resp.ScorePairArrayResult, *resp.Withscores, buf)
	}

	if resp.SliceArrayResult != nil || resp.HasEmptySliceArrayResult != nil {
		redis.WriteSliceArray(resp.SliceArrayResult, buf)
	}

	if resp.StatusResult != nil {
		redis.WriteStatus(resp.StatusResult, buf)
	}

	log.Debugf("redis-[%s]: response normal, resp=<%+v>",
		rs.addr,
		resp)
}
