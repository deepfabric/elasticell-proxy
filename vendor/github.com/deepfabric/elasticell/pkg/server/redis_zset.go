// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
)

func (s *RedisServer) onZAdd(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZCard(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZCount(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZIncrBy(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZLexCount(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRange(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRangeByLex(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRangeByScore(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRank(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRem(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRemRangeByLex(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRemRangeByRank(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZRemRangeByScore(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onZScore(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}
