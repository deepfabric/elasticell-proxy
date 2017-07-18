package proxy

import (
	"sync/atomic"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	credis "github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
	"golang.org/x/net/context"
)

type redisSession struct {
	ctx     context.Context
	cancel  context.CancelFunc
	session goetty.IOSession
	respsCh chan *raftcmdpb.Response
	retryCh chan *raftcmdpb.Request

	lastRetries int
	closed      int32
}

func newSession(session goetty.IOSession) *redisSession {
	ctx, cancel := context.WithCancel(context.TODO())

	return &redisSession{
		ctx:         ctx,
		cancel:      cancel,
		session:     session,
		respsCh:     make(chan *raftcmdpb.Response, 32),
		retryCh:     make(chan *raftcmdpb.Request, 1),
		lastRetries: 0,
	}
}

func (rs *redisSession) close() {
	if !rs.isClosed() {
		atomic.StoreInt32(&rs.closed, 1)
		rs.cancel()
		close(rs.respsCh)
		close(rs.retryCh)
		log.Infof("redis-[%s]: closed", rs.session.RemoteAddr())
	}
}

func (rs *redisSession) isClosed() bool {
	return atomic.LoadInt32(&rs.closed) == 1
}

func (rs *redisSession) addToRetry(req *raftcmdpb.Request) {
	rs.retryCh <- req
}

func (rs *redisSession) onResp(rsp *raftcmdpb.Response) {
	rs.respsCh <- rsp
}

func (rs *redisSession) retryLoop(p *RedisProxy) {
	for {
		select {
		case <-rs.ctx.Done():
			return
		case req := <-rs.retryCh:
			if req != nil {
				p.addToForward(rs, req)
			}
		}
	}
}

func (rs *redisSession) writeLoop() {
	for {
		select {
		case <-rs.ctx.Done():
			return
		case resp := <-rs.respsCh:
			if resp != nil {
				rs.doResp(resp)
			}
		}
	}
}

func (rs *redisSession) respError(err error) {
	buf := rs.session.OutBuf()
	redis.WriteError(util.StringToSlice(err.Error()), buf)
	rs.session.WriteOutBuf()

	log.Debugf("redis-[%s]: response error, err=<%+v>",
		rs.session.RemoteAddr(),
		err)
}

func (rs *redisSession) doResp(resp *raftcmdpb.Response) {
	buf := rs.session.OutBuf()

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

	rs.session.WriteOutBuf()

	log.Debugf("redis-[%s]: response normal, resp=<%+v>",
		rs.session.RemoteAddr(),
		resp)
}
