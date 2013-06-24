package cluster

import (
	"net"
	"sync"
	"time"
)

type Node struct {
	Inbox chan *Message

	tcp *net.TCPListener

	remotes map[string]Remote
	history map[string]Remote
	remlock *sync.RWMutex

	notify *callbacks
	recv   chan *message

	optlock      *sync.RWMutex
	durReconnect time.Duration
	durHealthy   time.Duration
	durNetwork   time.Duration

	wg              *sync.WaitGroup
	demultiplexQuit chan struct{}
	healthyQuit     chan struct{}
	reconnectQuit   chan struct{}
}

func New(laddr string) (*Node, error) {
	inbox := make(chan *Message, 100)
	n := &Node{
		Inbox:   inbox,
		tcp:     nil,
		remotes: make(map[string]Remote),
		history: make(map[string]Remote),
		remlock: new(sync.RWMutex),
		notify:  newCallbacks(),
		recv:    make(chan *message),

		optlock:      new(sync.RWMutex),
		durReconnect: 5 * time.Minute,
		durHealthy:   30 * time.Second,
		durNetwork:   10 * time.Second,

		wg:              new(sync.WaitGroup),
		demultiplexQuit: make(chan struct{}),
		healthyQuit:     make(chan struct{}),
		reconnectQuit:   make(chan struct{}),
	}

	addr, err := net.ResolveTCPAddr("tcp", laddr)
	if err != nil {
		return nil, err
	}
	if n.tcp, err = net.ListenTCP("tcp", addr); err != nil {
		return nil, err
	}
	go func() {
		defer n.tcp.Close()
		for {
			conn, err := n.tcp.AcceptTCP()
			if err != nil {
				n.debugf("Could not accept TCP connection: %s", err)
				break
			}
			go n.serve(conn)
		}
	}()

	go n.demultiplex()
	go n.healthy()
	go n.reconnect()

	return n, nil
}

// Addr returns the TCP address that the node is listening on.
func (n *Node) Addr() *net.TCPAddr {
	return n.tcp.Addr().(*net.TCPAddr)
}

func (n *Node) String() string {
	return n.Addr().String()
}

// Add joins the node to another node at the remote address specified.
func (n *Node) Add(raddr string) error {
	addr, err := net.ResolveTCPAddr("tcp", raddr)
	if err != nil {
		return err
	}
	return n.add(remote(addr))
}

// Broadcast sends the supplied message to every remote known by this node.
func (n *Node) Broadcast(payload []byte) {
	n.broadcast(msgUser, payload)
}

// Close gracefully shuts down this node from the cluster and returns only
// after all goroutines associated with the node have stopped.
// Other nodes will not attempt reconnection.
func (n *Node) Close() {
	n.broadcast(msgRemove, nil)
	close(n.Inbox)

	n.tcp.Close()
	n.healthyQuit <- struct{}{}
	n.reconnectQuit <- struct{}{}
	n.demultiplexQuit <- struct{}{}

	n.wg.Wait()
}

// CloseRemote gracefully closes a connection with the remote specified.
// No automatic reconnection will be made.
func (n *Node) CloseRemote(r Remote) {
	n.send(r, msgRemove, nil)
	n.unlearn(r)

	// Wipe it from the history too, so we don't attempt a reconnect.
	n.remlock.Lock()
	delete(n.history, r.String())
	n.remlock.Unlock()
}

// SetReconnectInterval specifies the interval at which reconnection is
// attempted with disconnected remotes. The default interval is 5 minutes.
//
// Note that reconnection only applies to remotes that were ungracefully
// disconnected from the cluster. A graceful disconnection can only happen
// by calling Close or CloseRemote.
func (n *Node) SetReconnectInterval(d time.Duration) {
	n.optlock.Lock()
	defer n.optlock.Unlock()

	n.durReconnect = d
}

func (n *Node) getReconnectInterval() time.Duration {
	n.optlock.RLock()
	defer n.optlock.RUnlock()

	return n.durReconnect
}

// SetHealthyInterval specifies how often the health of all remotes known
// by this node is checked. If a remote cannot receive a message, then it
// is ungracefully removed from known remotes. The default interval is
// 30 seconds.
func (n *Node) SetHealthyInterval(d time.Duration) {
	n.optlock.Lock()
	defer n.optlock.Unlock()

	n.durHealthy = d
}

func (n *Node) getHealthyInterval() time.Duration {
	n.optlock.RLock()
	defer n.optlock.RUnlock()

	return n.durHealthy
}

// SetNetworkTimeout specifies how long a TCP send or receive will wait before
// timing out the connection. If a remote times out, it is ungracefully
// removed from known remotes. The default interval is 10 seconds.
func (n *Node) SetNetworkTimeout(d time.Duration) {
	n.optlock.Lock()
	defer n.optlock.Unlock()

	n.durNetwork = d
}

func (n *Node) getNetworkTimeout() time.Duration {
	n.optlock.RLock()
	defer n.optlock.RUnlock()

	return n.durNetwork
}
