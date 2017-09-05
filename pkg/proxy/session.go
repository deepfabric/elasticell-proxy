package proxy

import (
	"sync/atomic"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	credis "github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

type redisSession struct {
	session goetty.IOSession
	resps   *queue.Queue

	addr   string
	closed int32
}

func newSession(session goetty.IOSession) *redisSession {
	return &redisSession{
		session: session,
		resps:   &queue.Queue{},
		addr:    session.RemoteAddr(),
	}
}

func (rs *redisSession) close() {
	if !rs.isClosed() {
		atomic.StoreInt32(&rs.closed, 1)
		rs.resps.Dispose()
		log.Infof("redis-[%s]: closed", rs.addr)
	}
}

func (rs *redisSession) isClosed() bool {
	return atomic.LoadInt32(&rs.closed) == 1
}

func (rs *redisSession) resp(rsp *raftcmdpb.Response) {
	if rs != nil && !rs.isClosed() {
		rs.resps.Put(rsp)
	}
}

func (rs *redisSession) errorResp(err error) {
	rs.resp(&raftcmdpb.Response{
		ErrorResult: util.StringToSlice(err.Error()),
	})
}

func (rs *redisSession) writeLoop() {
	for {
		resps, err := rs.resps.Get(batch)
		if nil != err {
			return
		}

		buf := rs.session.OutBuf()
		for _, resp := range resps {
			rs.doResp(resp.(*raftcmdpb.Response), buf)
		}
		rs.session.WriteOutBuf()
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
