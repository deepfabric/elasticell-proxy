package proxy

import (
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
}

func newSession(session goetty.IOSession) *redisSession {
	ctx, cancel := context.WithCancel(context.TODO())

	return &redisSession{
		ctx:     ctx,
		cancel:  cancel,
		session: session,
		respsCh: make(chan *raftcmdpb.Response, 32),
		retryCh: make(chan *raftcmdpb.Request, 1),
	}
}

func (rs *redisSession) close() {
	rs.cancel()
	log.Infof("redis-[%s]: closed", rs.session.RemoteAddr())
	close(rs.respsCh)
	close(rs.retryCh)
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
				if req.Epoch >= p.getSyncEpoch() {
					p.refreshRanges()
				}

				// already refresh, direct to retry
				p.handleReq(rs, req)
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
