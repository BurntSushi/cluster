package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
)

type discriminant int

const (
	msgUser discriminant = iota
	msgJoin
	msgJoinReply
)

func discriminantString(d discriminant) string {
	switch d {
	case msgUser:
		return "USER"
	case msgJoin:
		return "JOIN"
	case msgJoinReply:
		return "JOIN REPLY"
	}
	panic("bug")
}

type message struct {
	D       discriminant
	Payload []byte
}

func mesg(d discriminant, data interface{}) *message {
	// Special case the data to avoid unnecessary encoding/decoding.
	var payload []byte
	switch d := data.(type) {
	case []byte:
		payload = d
	case string:
		payload = []byte(d)
	default:
		b := new(bytes.Buffer)
		w := gob.NewEncoder(b)
		if err := w.Encode(data); err != nil {
			panic(err)
		}
		payload = b.Bytes()
	}
	return &message{d, payload}
}

func (m *message) decodePayload(v interface{}) {
	b := bytes.NewReader(m.Payload)
	r := gob.NewDecoder(b)
	if err := r.Decode(v); err != nil {
		panic(err)
	}
}

func (m *message) String() string {
	return fmt.Sprintf("(%s, %d bytes)",
		discriminantString(m.D), len(m.Payload))
}

func rawSend(conn *net.TCPConn, m *message) error {
	r := gob.NewEncoder(conn)
	if err := r.Encode(&m); err != nil {
		return err
	}
	return nil
}

func rawRecv(conn *net.TCPConn) (*message, error) {
	m := new(message)
	r := gob.NewDecoder(conn)
	if err := r.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}
