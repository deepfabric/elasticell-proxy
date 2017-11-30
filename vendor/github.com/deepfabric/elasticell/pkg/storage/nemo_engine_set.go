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

// +build freebsd openbsd netbsd dragonfly linux

package storage

import (
	"github.com/deepfabric/elasticell/pkg/util"
	gonemo "github.com/deepfabric/go-nemo"
	"golang.org/x/net/context"
)

type nemoSetEngine struct {
	limiter *util.Limiter
	db      *gonemo.NEMO
}

func newNemoSetEngine(db *gonemo.NEMO, cfg *NemoCfg) SetEngine {
	return &nemoSetEngine{
		limiter: util.NewLimiter(cfg.LimitConcurrencyWrite),
		db:      db,
	}
}

func (e *nemoSetEngine) SAdd(key []byte, members ...[]byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.SAdd(key, members...)
	e.limiter.Release()

	return n, err
}

func (e *nemoSetEngine) SRem(key []byte, members ...[]byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.SRem(key, members...)
	e.limiter.Release()

	return n, err
}

func (e *nemoSetEngine) SCard(key []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.SCard(key)
	e.limiter.Release()

	return n, err
}

func (e *nemoSetEngine) SMembers(key []byte) ([][]byte, error) {
	return e.db.SMembers(key)
}

func (e *nemoSetEngine) SIsMember(key []byte, member []byte) (int64, error) {
	yes, err := e.db.SIsMember(key, member)
	var value int64
	if yes {
		value = 1
	}

	return value, err
}

func (e *nemoSetEngine) SPop(key []byte) ([]byte, error) {
	e.limiter.Wait(context.TODO())
	exists, value, err := e.db.SPop(key)
	e.limiter.Release()

	if !exists {
		return nil, err
	}

	return value, err
}
