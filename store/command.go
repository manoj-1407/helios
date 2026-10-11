package store

import (
	"encoding/binary"
	"fmt"
)

type CmdType uint8

const (
	CmdPut    CmdType = 1
	CmdDelete CmdType = 2
)

type Command struct {
	Type  CmdType
	Key   string
	Value string
}

func EncodeCommand(cmd Command) []byte {
	klen := len(cmd.Key)
	vlen := len(cmd.Value)
	buf := make([]byte, 1+4+klen+4+vlen)
	buf[0] = byte(cmd.Type)
	binary.BigEndian.PutUint32(buf[1:], uint32(klen))
	copy(buf[5:], cmd.Key)
	binary.BigEndian.PutUint32(buf[5+klen:], uint32(vlen))
	copy(buf[9+klen:], cmd.Value)
	return buf
}

func DecodeCommand(data []byte) (Command, error) {
	if len(data) < 9 {
		return Command{}, fmt.Errorf("command too short: %d bytes", len(data))
	}
	cmdType := CmdType(data[0])
	klen := int(binary.BigEndian.Uint32(data[1:]))
	if len(data) < 5+klen+4 {
		return Command{}, fmt.Errorf("truncated key")
	}
	key := string(data[5 : 5+klen])
	vlen := int(binary.BigEndian.Uint32(data[5+klen:]))
	if len(data) < 9+klen+vlen {
		return Command{}, fmt.Errorf("truncated value")
	}
	val := string(data[9+klen : 9+klen+vlen])
	return Command{Type: cmdType, Key: key, Value: val}, nil
}
