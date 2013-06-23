package cluster

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

var (
	lg *log.Logger
)

func init() {
	lg = log.New(os.Stderr, "[cluster] ", log.LstdFlags)
}

type Cluster struct {
	tcp *net.TCPListener

	remotes       map[string]*Remote
	remotesLocker *sync.Mutex

	connLimit       int
	connLimitLocker *sync.Mutex
}

// New creates a new cluster on the given address, which can
// connect to other clusters. The address should be in the format "host:port".
//
// If the port is zero, then an open port will be chosen for you. It can be
// discovered via the Addr method.
func New(laddr string) (*Cluster, error) {
	c := &Cluster{
		tcp:             nil,
		remotes:         make(map[string]*Remote),
		remotesLocker:   new(sync.Mutex),
		connLimit:       1,
		connLimitLocker: new(sync.Mutex),
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
	return c, nil
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

	// A handshake with an incoming remote connection has two steps:
	// 1) The incoming connection sends a JOIN message with a payload that
	// contains its cluster's TCP address, which acts as a uniquely
	// identifying key.
	// 2) This cluster responds with an OK message if the connection is
	// valid
	handshake, err := rawRecv(conn)
	if err != nil {
		return err
	}

	var remoteAddr *net.TCPAddr
	handshake.decodePayload(&remoteAddr)
	key := addrString(remoteAddr)

	r := newRemote(c, remoteAddr)
	if err := rawSend(conn, mesg(msgJoinReply, "OK")); err != nil {
		return err
	}

	// If the remote doesn't exist yet, we need to make it first.
	r, ok := c.remotes[key]
	if !ok {
		r = newRemote(c, remoteAddr)
		c.remotes[key] = r
	}
	r.addConn(conn)

	c.debugf("Handshake for (%s, %s) is complete.", c, r)
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
