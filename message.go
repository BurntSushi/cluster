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
	msgWhoDoYouKnow
	msgIKnow
)

func discriminantString(d discriminant) string {
	switch d {
	case msgUser:
		return "USER"
	case msgJoin:
		return "JOIN"
	case msgJoinReply:
		return "JOIN REPLY"
	case msgWhoDoYouKnow:
		return "WHO DO YOU KNOW"
	case msgIKnow:
		return "I KNOW"
	}
	panic("bug")
}

type Message struct {
	Payload []byte
	From *Remote
}

type message struct {
	D       discriminant
	Payload []byte
	To *net.TCPAddr
}

type remoteMessage struct {
	From *Remote
	*message
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
	return &message{d, payload, nil}
}

func (m *message) decodePayload(v interface{}) {
	b := bytes.NewReader(m.Payload)
	r := gob.NewDecoder(b)
	if err := r.Decode(v); err != nil {
		panic(err)
	}
}

func (m *message) String() string {
	return fmt.Sprintf("(%s, to: %s, %d bytes)",
		discriminantString(m.D), m.To, len(m.Payload))
}

func rawSend(conn *net.TCPConn, enc *gob.Encoder, m *message) error {
	if err := enc.Encode(&m); err != nil {
		return err
	}
	return nil
}

func rawRecv(conn *net.TCPConn, dec *gob.Decoder) (*message, error) {
	m := new(message)
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}
