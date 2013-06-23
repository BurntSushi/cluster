package cluster

import (
	"encoding/gob"
	"fmt"
	"net"
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

func newRemote(cluster *Cluster, addr *net.TCPAddr) *Remote {
	r := &Remote{
		cluster:     cluster,
		addr:        addr,
		conns:       make([]*net.TCPConn, 0),
		connsLocker: new(sync.Mutex),
		send:        make(chan *message),
		recv:        make(chan *message),
	}
	// go r.healthy()
	return r
}

// func (r *Remote) healthy() {
// for _ = range time.Tick(5 * time.Second) {
// r.debugf("health check")
// }
// }

// Add joins a remote node to the cluster and initializes the TCP connection
// pool.
//
// Any other remote nodes known by the new node will also be added to the
// current cluster, if possible.
func (c *Cluster) Add(raddr string) error {
	c.remotesLocker.Lock()
	defer c.remotesLocker.Unlock()

	addr, err := net.ResolveTCPAddr("tcp", raddr)
	if err != nil {
		return err
	}

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

	// Send initial join message.
	handshake := mesg(msgJoin, c.Addr())
	if err := rawSend(conn, handshake); err != nil {
		conn.Close()
		return err
	}

	// Receive the response and make sure it's kosher.
	resp, err := rawRecv(conn)
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
	c.remotes[key] = r
	r.addConn(conn)

	c.debugf("Handshake for (%s, %s) is complete.", c, r)
	return nil
}

func (r *Remote) addConn(conn *net.TCPConn) {
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
		reader := gob.NewDecoder(conn)
		for {
			m := new(message)
			if err := reader.Decode(&m); err != nil {
				r.logf("TCP read error for %s: %s", r, err)
				readError <- struct{}{}
				break
			}
			r.recv <- m
		}
	}()
	go func() {
		defer wg.Done()
		writer := gob.NewEncoder(conn)
		for {
			select {
			case msg := <-r.send:
				if err := writer.Encode(msg); err != nil {
					r.logf("TCP write error for %s: %s", r.addr, err)

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
		for i, other := range r.conns {
			if conn == other {
				r.debugf("Removing TCP connection for %s", r)
				r.conns = append(r.conns[:i], r.conns[i+1:]...)
				return
			}
		}
	}()

}

func (r *Remote) String() string {
	return addrString(r.addr)
}

func (r *Remote) logf(format string, v ...interface{}) {
	e := fmt.Sprintf(format, v...)
	lg.Printf("Error for remote '%s': %s", r.String(), e)
}

func (r *Remote) debugf(format string, v ...interface{}) {
	e := fmt.Sprintf(format, v...)
	lg.Printf("DEBUG for remote '%s': %s", r.String(), e)
}
