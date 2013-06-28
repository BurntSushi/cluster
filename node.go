package cluster

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

var (
	lg *log.Logger
)

func init() {
	lg = log.New(os.Stderr, "[cluster] ", log.Ltime)
}

func (n *Node) broadcast(d discriminant, payload []byte) {
	n.remlock.RLock()
	defer n.remlock.RUnlock()

	for _, r := range n.remotes {
		if err := n.send(r, d, payload); err != nil {
			n.logf("Broadcast to '%s' failed: %s", r, err)
		}
	}
}

func (n *Node) add(r Remote) error {
	data, err := payload(r)
	if err != nil {
		return fmt.Errorf("Could not GOB encode remote '%s': %s", r, err)
	}
	if err := n.send(r, msgJoin, data); err != nil {
		return err
	}
	return nil
}

func (n *Node) healthy() {
	n.wg.Add(1)
	defer n.wg.Done()

	check := func() {
		n.remlock.RLock()
		defer n.remlock.RUnlock()

		for _, r := range n.remotes {
			lastheard, ok := n.lastheard[r.String()]
			if !ok {
				n.logf("Never heard from '%s'. Disconnecting.", r)

				// Do a graceful disconnect since we've never heard from this
				// remote. (A bug, perhaps?)
				go n.CloseRemote(r)
				continue
			} else if time.Since(lastheard) > n.getHealthyInterval()*2 {
				n.debugf("Remote '%s' hasn't responded in %s. Severing.", r,
					n.getHealthyInterval()*2)

				// Do an ungraceful disconnect since we might want to try
				// reconnecting later.
				go n.unlearn(r, false)
				continue
			}
			if err := n.send(r, msgHealthy, nil); err != nil {
				continue
			}
		}
	}
	for {
		select {
		case <-n.healthyQuit:
			return
		case <-time.After(n.getHealthyInterval()):
			check()
		}
	}
}

func (n *Node) reconnect() {
	n.wg.Add(1)
	defer n.wg.Done()

	check := func() {
		n.remlock.RLock()
		defer n.remlock.RUnlock()

		for key, r := range n.history {
			if _, ok := n.remotes[key]; ok {
				continue
			}
			if err := n.add(r); err != nil {
				n.debugf("Could not reconnect with '%s': %s", r, err)
			}
		}
	}
	for {
		select {
		case <-n.reconnectQuit:
			return
		case <-time.After(n.getReconnectInterval()):
			check()
		}
	}
}

func (n *Node) serve(conn *net.TCPConn) {
	defer conn.Close()

	m, err := n.receive(conn)
	if err != nil {
		n.logf("Could not receive from '%s': %s", connString(conn), err)
		return
	}
	n.recv <- m
}

func (n *Node) demultiplex() {
	n.wg.Add(1)
	defer n.wg.Done()

	for {
		select {
		case <-n.demultiplexQuit:
			return
		case msg := <-n.recv:
			n.remlock.Lock()
			n.lastheard[msg.From.String()] = time.Now()
			n.remlock.Unlock()

			switch msg.D {
			case msgJoin:
				err := n.send(msg.From, msgJoinReply, msg.Payload)
				if err != nil {
					n.logf("Could not respond to JOIN from '%s': %s",
						msg.From, err)
				} else {
					n.learn(msg.From)
				}
			case msgJoinReply:
				var byWayOf Remote
				if err := msg.decodePayload(&byWayOf); err != nil {
					n.logf("Couldn't decode payload in '%s': %s", msg, err)
					n.logf("Could not process JOIN from '%s'.", msg.From)
					continue
				}

				// Upon sending an initial JOIN message, sometimes we connect
				// through a different address (e.g., an ssh tunnel).
				// Thus, the regular "remote" address of the TCP connection
				// isn't good enough to know the right way to send a message.
				// Therefore, every JOIN message includes the initial address
				// used for connection. The JOIN_REPLY message sends the
				// initial address back, and we add it to the map here.
				// Every subsequent send checks this table and uses the
				// "by way of" address if it exists.
				if !msg.From.equal(byWayOf) {
					n.remlock.Lock()
					n.debugf("Setting '%s' by way of '%s'", msg.From, byWayOf)
					n.bywayof[msg.From.String()] = byWayOf
					n.remlock.Unlock()
				}

				n.learn(msg.From)
			case msgHealthy:
				if !n.knows(msg.From) {
					n.send(msg.From, msgUnknown, nil)
				} else {
					n.send(msg.From, msgKeepAlive, nil)
				}
			case msgUnknown:
				if n.knows(msg.From) {
					n.add(msg.From)
				}
			default:
				if n.knows(msg.From) {
					n.handle(msg)
				} else {
					n.logf("Ignoring message %s from unknown remote '%s'.",
						msg.D, msg.From)
				}
			}
		}
	}
}

// handle demultiplexes a message based on its discriminant.
// precondition: the remote that sent the message must be known to the node.
// i.e., it has already JOINed.
func (n *Node) handle(msg *message) {
	switch msg.D {
	case msgUser:
		n.Inbox <- &Message{msg.From, msg.Payload}
	case msgRemove:
		// A graceful remove, so wipe the history.
		n.unlearn(msg.From, true)
	case msgWhoDoYouKnow:
		n.shareKnowledge(msg.From)
	case msgIKnow:
		var remotes []Remote
		if err := msg.decodePayload(&remotes); err != nil {
			n.logf("Couldn't decode payload in '%s': %s", msg, err)
			return
		}
		for _, r := range remotes {
			n.add(r)
		}
	case msgKeepAlive:
		// Nothing to see here. A keep alive works by updating "last heard".
	default:
		n.logf("Unknown message from '%s': %s", msg.From, msg.D)
	}
}

func (n *Node) shareKnowledge(r Remote) {
	n.remlock.RLock()
	defer n.remlock.RUnlock()

	rs := make([]Remote, 0)
	for _, other := range n.remotes {
		if !r.equal(other) {
			rs = append(rs, other)
		}
	}
	data, err := payload(rs)
	if err != nil {
		n.logf("Problem sharing knowledge with '%s': %s", r, err)
		return
	}
	n.send(r, msgIKnow, data)
}

func (n *Node) knows(r Remote) bool {
	n.remlock.RLock()
	defer n.remlock.RUnlock()

	if r.equal(remote(n.Addr())) {
		return true
	}
	_, ok := n.remotes[r.String()]
	return ok
}

func (n *Node) inHistory(r Remote) bool {
	n.remlock.RLock()
	defer n.remlock.RUnlock()

	if r.equal(remote(n.Addr())) {
		return true
	}
	_, ok := n.history[r.String()]
	return ok
}

func (n *Node) learn(r Remote) {
	n.remlock.Lock()
	defer n.remlock.Unlock()

	if r.equal(remote(n.Addr())) {
		return
	}
	if _, ok := n.remotes[r.String()]; ok {
		n.debugf("Remote '%s' is already known.", r)
		return
	}

	n.remotes[r.String()] = r
	n.history[r.String()] = r
	n.runRemoteAdded(r)
	n.runRemoteChanged()
	go n.send(r, msgWhoDoYouKnow, nil)

	n.debugf("Learned new remote: %s", r)
}

func (n *Node) unlearn(r Remote, wipeHistory bool) {
	n.remlock.Lock()
	defer n.remlock.Unlock()

	if _, ok := n.remotes[r.String()]; !ok {
		return
	}

	delete(n.remotes, r.String())
	if wipeHistory {
		delete(n.history, r.String())
		delete(n.lastheard, r.String())
		// holding on to n.bywayof for now...
	}
	n.runRemoteRemoved(r)
	n.runRemoteChanged()

	n.debugf("Unlearned remote: %s", r)
}

type Remote net.TCPAddr

func remote(addr *net.TCPAddr) Remote {
	return Remote(*addr)
}

func (r Remote) addr() *net.TCPAddr {
	a := net.TCPAddr(r)
	return &a
}

func (r Remote) String() string {
	return r.addr().String()
}

func (r1 Remote) equal(r2 Remote) bool {
	return r1.IP.Equal(r2.IP) && r1.Port == r2.Port && r1.Zone == r2.Zone
}

func (n *Node) logf(format string, v ...interface{}) {
	e := fmt.Sprintf(format, v...)
	lg.Printf("ERROR '%s': %s", n.String(), e)
}

func (n *Node) debugf(format string, v ...interface{}) {
	if n.getDebug() {
		e := fmt.Sprintf(format, v...)
		lg.Printf("DEBUG '%s': %s", n.String(), e)
	}
}

func connString(conn *net.TCPConn) string {
	return fmt.Sprintf("(%s, %s)", conn.LocalAddr(), conn.RemoteAddr())
}
