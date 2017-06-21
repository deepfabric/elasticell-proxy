package proxy

import (
	"hash/crc32"
	"sync"

	"github.com/deepfabric/elasticell/pkg/util"
)

const (
	bucketSize = 128
	bucketM    = 127
)

type routingMap struct {
	sync.RWMutex
	m map[string]*redisSession
}

func (m *routingMap) put(key string, value *redisSession) {
	m.Lock()
	m.m[key] = value
	m.Unlock()
}

func (m *routingMap) delete(key string) *redisSession {
	m.Lock()
	value := m.m[key]
	delete(m.m, key)
	m.Unlock()

	return value
}

type routing struct {
	rms []*routingMap
}

func newRouting() *routing {
	r := &routing{
		rms: make([]*routingMap, bucketSize, bucketSize),
	}

	for i := 0; i < bucketSize; i++ {
		r.rms[i] = &routingMap{
			m: make(map[string]*redisSession),
		}
	}

	return r
}

func (r *routing) put(uuid []byte, value *redisSession) {
	r.rms[getIndex(uuid)].put(util.SliceToString(uuid), value)
}

func (r *routing) delete(uuid []byte) *redisSession {
	return r.rms[getIndex(uuid)].delete(util.SliceToString(uuid))
}

func getIndex(key []byte) int {
	return int(crc32.ChecksumIEEE(key) & bucketM)
}
