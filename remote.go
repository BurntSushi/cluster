package cluster

import (
	"encoding/gob"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Remote struct {
	cluster *Cluster

	// The address that the remote cluster is listening on.
	// This serves as a uniquely identifying key.
	addr *net.TCPAddr

	conns       []*net.TCPConn
	connsLocker *sync.Mutex

	send chan *message
	recv chan *message
}

// newRemote creates a new remote for the given cluster and updates the
// cluster's knowledge of that remote. It must be called while the
// remote locker is held.
//
// Also, a search for additional remotes is provoked.
func newRemote(cluster *Cluster, addr *net.TCPAddr) *Remote {
	r := &Remote{
		cluster:     cluster,
		addr:        addr,
		conns:       make([]*net.TCPConn, 0),
		connsLocker: new(sync.Mutex),
		send:        make(chan *message),
		recv:        make(chan *message),
	}
	cluster.remotes[addrString(addr)] = r
	cluster.history[addrString(addr)] = addr

	// Route all data received from this remote to the cluster.
	go func() {
		for msg := range r.recv {
			cluster.recv <- remoteMessage{r, msg}
		}
	}()
	return r
}

// Close cuts off the connection between the cluster and this remote.
// All TCP connections are closed.
func (r *Remote) Close() error {
	r.connsLocker.Lock()
	defer r.connsLocker.Unlock()

	close(r.send)
	errs := make([]string, 0)
	for _, conn := range r.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s\n", strings.Join(errs, "\n"))
	}
	return nil
}

// sever is a wrapper around Close that is called internally as a goroutine.
func (r *Remote) sever() {
	r.cluster.remotesLocker.Lock()
	defer r.cluster.remotesLocker.Unlock()

	if err := r.Close(); err != nil {
		r.logf(err.Error())
	}
	delete(r.cluster.remotes, addrString(r.addr))
	r.debugf("REMOVED.")
}

// Add joins a remote node to the cluster and initializes the TCP connection
// pool.
//
// Any other remote nodes known by the new node will also be added to the
// current cluster, if possible.
func (c *Cluster) Add(raddr string) error {
	addr, err := net.ResolveTCPAddr("tcp", raddr)
	if err != nil {
		return err
	}
	return c.add(addr)
}

func (c *Cluster) add(addr *net.TCPAddr) error {
	c.remotesLocker.Lock()
	defer c.remotesLocker.Unlock()

	// If this remote already exists, then there's no need to continue.
	key := addrString(addr)
	if _, ok := c.remotes[key]; ok {
		return nil
	}

	// Try to make an initial connection before creating the remote.
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	// Send initial join message.
	handshake := mesg(msgJoin, c.Addr())
	if err := rawSend(conn, enc, handshake); err != nil {
		conn.Close()
		return err
	}

	// Receive the response and make sure it's kosher.
	resp, err := rawRecv(conn, dec)
	if err != nil {
		conn.Close()
		return err
	}
	if msg := string(resp.Payload); msg != "OK" {
		conn.Close()
		return fmt.Errorf("Unexpected JOIN REPLY message: %s", msg)
	}

	// All is well. Create the remote seeded with one connection.
	// More connections will be added later.
	r := newRemote(c, addr)
	r.addConn(conn, enc, dec)
	go c.broadcast(mesg(msgIKnow, []*net.TCPAddr{r.addr}))

	c.debugf("Handshake for (%s, %s) is complete.", c, r)
	return nil
}

func (r *Remote) addConn(
	conn *net.TCPConn,
	enc *gob.Encoder,
	dec *gob.Decoder,
) {
	r.connsLocker.Lock()
	defer r.connsLocker.Unlock()

	if len(r.conns) >= r.cluster.MaxConns(0) {
		r.logf("Max TCP connections exceeded. Rejecting '%s'.",
			conn.RemoteAddr())
		return
	}
	r.conns = append(r.conns, conn)

	readError := make(chan struct{}, 1)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			m := new(message)
			if err := dec.Decode(&m); err != nil {
				r.logf("TCP read error: %s", err)
				readError <- struct{}{}
				break
			}
			r.recv <- m
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-r.send:
				msg.To = r.addr
				if err := enc.Encode(msg); err != nil {
					r.logf("TCP write error: %s", err)

					// Close the connection to make sure any pending read
					// fails.
					conn.Close()
					return
				}
			case <-readError:
				return
			}
		}
	}()

	// Clean up a connection if it is closed.
	go func() {
		wg.Wait()

		r.connsLocker.Lock()
		defer r.connsLocker.Unlock()

		conn.Close()
		close(r.recv)
		for i, other := range r.conns {
			if conn == other {
				r.debugf("Removing a TCP connection")
				r.conns = append(r.conns[:i], r.conns[i+1:]...)
				break
			}
		}

		// If there are no TCP connections left, close this remote down.
		if len(r.conns) == 0 {
			go r.sever()
		}
	}()

}

func (r *Remote) String() string {
	return addrString(r.addr)
}

func (r *Remote) logf(format string, v ...interface{}) {
	e := fmt.Sprintf(format, v...)
	lg.Printf("Error for remote (%s, %s): %s", r.cluster, r.String(), e)
}

func (r *Remote) debugf(format string, v ...interface{}) {
	e := fmt.Sprintf(format, v...)
	lg.Printf("DEBUG for remote (%s, %s): %s", r.cluster, r.String(), e)
}
