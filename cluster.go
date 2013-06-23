package cluster

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	lg *log.Logger
)

func init() {
	lg = log.New(os.Stderr, "[cluster] ", log.Ltime)
}

type Cluster struct {
	tcp *net.TCPListener

	remotes       map[string]*Remote
	remotesLocker *sync.Mutex

	// A history of every remote ever connected to during this session.
	// Periodically, a reconnection with any remote in the history currently
	// disconnected will be performed.
	history        map[string]*net.TCPAddr
	historicalQuit chan struct{}

	connLimit       int
	connLimitLocker *sync.Mutex

	explorerQuit chan struct{} // quits the explorer

	// All messages (including internal) received come through here.
	recv chan remoteMessage

	// Messages can be received by reading from this channel.
	Inbox <-chan *Message
}

// New creates a new cluster on the given address, which can
// connect to other clusters. The address should be in the format "host:port".
//
// If the port is zero, then an open port will be chosen for you. It can be
// discovered via the Addr method.
func New(laddr string) (*Cluster, error) {
	inbox := make(chan *Message, 100)
	c := &Cluster{
		tcp:             nil,
		remotes:         make(map[string]*Remote),
		remotesLocker:   new(sync.Mutex),
		history:         make(map[string]*net.TCPAddr),
		historicalQuit:  make(chan struct{}),
		connLimit:       1,
		connLimitLocker: new(sync.Mutex),
		explorerQuit:    make(chan struct{}, 1),
		recv:            make(chan remoteMessage),
		Inbox:           inbox,
	}

	addr, err := net.ResolveTCPAddr("tcp", laddr)
	if err != nil {
		return nil, err
	}
	if c.tcp, err = net.ListenTCP("tcp", addr); err != nil {
		return nil, err
	}
	go func() {
		defer c.tcp.Close()
		for {
			conn, err := c.tcp.AcceptTCP()
			if err != nil {
				lg.Printf("Could not accept TCP connection: %s", err)
				break
			}
			go c.serve(conn)
		}
	}()

	go c.historical()
	go c.explorer()
	go c.demultiplex(inbox)
	return c, nil
}

// historical tries to reconnect with any disconnected remotes.
func (c *Cluster) historical() {
	reconnect := func() {
		c.remotesLocker.Lock()
		defer c.remotesLocker.Unlock()

		c.debugf("Looking at history for reconnection opportunities...")

		for key, addr := range c.history {
			if _, ok := c.remotes[key]; ok {
				continue
			}
			go func() {
				if err := c.add(addr); err != nil {
					c.debugf("Could not reconnect with '%s': %s", key, err)
				}
			}()
		}
	}
	for {
		select {
		case <-time.After(10 * time.Second):
			reconnect()
		case <-c.historicalQuit:
			return
		}
	}
}

// demultiplex teases out incoming messages.
func (c *Cluster) demultiplex(inbox chan *Message) {
	for rmsg := range c.recv {
		switch rmsg.D {
		case msgUser:
			inbox <- &Message{rmsg.Payload, rmsg.From}
		case msgWhoDoYouKnow:
			c.sendRemotes(rmsg.From)
		case msgIKnow:
			var addrs []*net.TCPAddr
			rmsg.decodePayload(&addrs)
			c.learnRemotes(rmsg.To, addrs)
		default:
			c.logf("Unrecognized discriminant: %s", rmsg.D)
		}
	}
	close(inbox)
}

func (c *Cluster) learnRemotes(to *net.TCPAddr, addrs []*net.TCPAddr) {
	for _, addr := range addrs {
		if addrEqual(to, addr) {
			continue
		}
		if err := c.add(addr); err != nil {
			c.debugf("Could not connect with '%s': %s", addrString(addr), err)
		}
	}
}

func (c *Cluster) sendRemotes(r *Remote) {
	c.remotesLocker.Lock()
	defer c.remotesLocker.Unlock()

	addrs := make([]*net.TCPAddr, 0)
	for _, remote := range c.remotes {
		if r != remote {
			addrs = append(addrs, remote.addr)
		}
	}
	r.send <- mesg(msgIKnow, addrs)
}

// explorer is a goroutine that asks remotes who they know.
// It uses some clever tricks to try and find remotes quickly.
func (c *Cluster) explorer() {
	for {
		select {
		case <-time.After(60 * time.Second):
			c.broadcast(mesg(msgWhoDoYouKnow, 0))
		case <-c.explorerQuit:
			return
		}
	}
}

// broadcast sends a message to all known remotes.
func (c *Cluster) broadcast(m *message) {
	c.remotesLocker.Lock()
	defer c.remotesLocker.Unlock()

	for _, r := range c.remotes {
		r.send <- m
	}
}

// MaxConns sets or retrieves the maximum number of TCP connections allowed
// between two nodes. If `n` is zero or less, then the current limit is
// eturned. Any other value sets to the limit to `n`.
func (c *Cluster) MaxConns(n int) int {
	c.connLimitLocker.Lock()
	defer c.connLimitLocker.Unlock()

	if n <= 0 {
		return c.connLimit
	}
	c.connLimit = n
	return 0
}

// Addr returns the TCP address that the cluster is listening on.
func (c *Cluster) Addr() *net.TCPAddr {
	return c.tcp.Addr().(*net.TCPAddr)
}

// String returns the canonical "ip:port" of the cluster.
func (c *Cluster) String() string {
	return addrString(c.Addr())
}

func (c *Cluster) serve(conn *net.TCPConn) {
	if err := c.handshake(conn); err != nil {
		c.logf("Invalid handshake for '%s': %s", connString(conn), err)
		conn.Close()
		return
	}
}

func (c *Cluster) handshake(conn *net.TCPConn) error {
	c.remotesLocker.Lock()
	defer c.remotesLocker.Unlock()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	// A handshake with an incoming remote connection has two steps:
	// 1) The incoming connection sends a JOIN message with a payload that
	// contains its cluster's TCP address, which acts as a uniquely
	// identifying key.
	// 2) This cluster responds with an OK message if the connection is
	// valid
	handshake, err := rawRecv(conn, dec)
	if err != nil {
		return err
	}

	var remoteAddr *net.TCPAddr
	handshake.decodePayload(&remoteAddr)
	key := addrString(remoteAddr)

	if err := rawSend(conn, enc, mesg(msgJoinReply, "OK")); err != nil {
		return err
	}

	// If the remote doesn't exist yet, we need to make it first.
	r, ok := c.remotes[key]
	if !ok {
		r = newRemote(c, remoteAddr)
	}
	r.addConn(conn, enc, dec)
	go c.broadcast(mesg(msgIKnow, []*net.TCPAddr{r.addr}))

	c.debugf("Handshake for (%s, %s) is complete.", c, r)
	return nil
}

// Close kills all connections with other clusters.
func (c *Cluster) Close() error {
	c.remotesLocker.Lock()
	defer c.remotesLocker.Unlock()

	errs := make([]string, 0)
	if err := c.tcp.Close(); err != nil {
		errs = append(errs, err.Error())
	}
	c.historicalQuit <- struct{}{}
	c.explorerQuit <- struct{}{}
	for _, r := range c.remotes {
		if err := r.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	close(c.recv)
	if len(errs) > 0 {
		return fmt.Errorf("%s\n", strings.Join(errs, "\n"))
	}
	return nil
}

func (c *Cluster) logf(format string, v ...interface{}) {
	e := fmt.Sprintf(format, v...)
	lg.Printf("Error for cluster '%s': %s", c.String(), e)
}

func (c *Cluster) debugf(format string, v ...interface{}) {
	e := fmt.Sprintf(format, v...)
	lg.Printf("DEBUG for cluster '%s': %s", c.String(), e)
}

func connString(conn *net.TCPConn) string {
	laddr := addrString(conn.LocalAddr().(*net.TCPAddr))
	raddr := addrString(conn.RemoteAddr().(*net.TCPAddr))
	return fmt.Sprintf("(%s, %s)", laddr, raddr)
}

func addrString(addr *net.TCPAddr) string {
	return fmt.Sprintf("%s:%d", addr.IP, addr.Port)
}

func addrEqual(a1, a2 *net.TCPAddr) bool {
	return a1.IP.Equal(a2.IP) && a1.Port == a2.Port && a1.Zone == a2.Zone
}
