package proxy

import (
	"errors"
	"fmt"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

var (
	// ErrIllegalPacket parse err
	ErrIllegalPacket = errors.New("illegal packet data")
)

const (
	// ProxyBegin 0x01
	ProxyBegin = 0x01
)

type redisDecoder struct {
}

func (decoder *redisDecoder) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	for {
		// remember the begin read index,
		// if we found has no enough data, we will resume this read index,
		// and waiting for next.
		backupReaderIndex := in.GetReaderIndex()

		c, err := in.ReadByte()
		if err != nil {
			return false, nil, err
		}

		if c != ProxyBegin {
			return false, nil, ErrIllegalPacket
		}

		return readCommandByProxyProtocol(in, backupReaderIndex)
	}
}

type redisEncoder struct {
}

func (encoder *redisEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	req := data.(*raftcmdpb.Request)
	value, err := req.Marshal()
	if err != nil {
		return err
	}

	out.WriteByte(ProxyBegin)
	out.WriteInt(len(value))
	out.Write(value)

	return nil
}

func readCommandByProxyProtocol(in *goetty.ByteBuf, backupReaderIndex int) (bool, interface{}, error) {
	if in.Readable() < 4 {
		in.SetReaderIndex(backupReaderIndex)
		return false, nil, nil
	}

	size, _ := in.PeekInt(0)
	if in.Readable() < 4+size {
		in.SetReaderIndex(backupReaderIndex)
		return false, nil, nil
	}

	in.Skip(4)
	n, data, err := in.ReadBytes(size)
	if err != nil {
		return false, nil, err
	}

	if n != size {
		return false, nil, fmt.Errorf("read bytes not match length field, expect=<%d>, read=<%d>", size, n)
	}

	resp := new(raftcmdpb.Response)
	util.MustUnmarshal(resp, data)
	return true, resp, nil
}
