package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"time"
)

type discriminant int

const (
	msgJoin discriminant = iota
	msgJoinReply
	msgRemove
	msgUnknown
	msgWhoDoYouKnow
	msgIKnow
	msgHealthy
	msgUser
)

func (d discriminant) String() string {
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
	case msgHealthy:
		return "HEALTHY"
	}
	panic("bug")
}

// Message represents data sent to your node from another node.
// They can be retrieved via the Inbox channel of the corresponding node.
type Message struct {
	// The remote address of the sender.
	From Remote

	// The content of the message.
	Payload []byte
}

type message struct {
	D       discriminant
	Payload []byte
	From    Remote
	To      Remote
}

func (n *Node) destination(r Remote) Remote {
	n.remlock.RLock()
	defer n.remlock.RUnlock()

	if bywayof, ok := n.bywayof[r.String()]; ok {
		return bywayof
	}
	return r
}

func (n *Node) send(to Remote, d discriminant, data []byte) error {
	if d > msgUnknown && !n.knows(to) {
		return fmt.Errorf("I do not know about remote '%s'.", to)
	}

	dest := n.destination(to)
	conn, err := net.DialTimeout("tcp", dest.String(), n.getNetworkTimeout())
	if err != nil {
		go n.unlearn(to, false)
		return err
	}
	defer conn.Close()

	m := &message{d, data, remote(n.Addr()), to}
	enc := gob.NewEncoder(conn)

	deadline := time.Now().Add(n.getNetworkTimeout())
	if err := conn.SetWriteDeadline(deadline); err != nil {
		go n.unlearn(to, false)
		return err
	}
	if err := enc.Encode(&m); err != nil {
		if isNetError(err) {
			go n.unlearn(to, false)
		}
		return err
	}
	return nil
}

func isNetError(err error) bool {
	if _, ok := err.(net.Error); ok {
		return true
	}
	return false
}

func (n *Node) receive(conn *net.TCPConn) (*message, error) {
	dec := gob.NewDecoder(conn)
	m := new(message)

	deadline := time.Now().Add(n.getNetworkTimeout())
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func payload(data interface{}) ([]byte, error) {
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
			return nil, err
		}
		payload = b.Bytes()
	}
	return payload, nil
}

func (m *message) decodePayload(v interface{}) error {
	b := bytes.NewReader(m.Payload)
	r := gob.NewDecoder(b)
	if err := r.Decode(v); err != nil {
		return err
	}
	return nil
}
