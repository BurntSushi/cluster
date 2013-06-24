package cluster

import (
	"net"
	"sync"
	"time"
)

// Node corresponds to a single local entity in a cluster. Messages sent to
// this node must be retrieved by reading from the Inbox channel. Note that if
// messages are never read, the channel buffer will fill and the Node will
// stop functioning until messages are drained.
//
// There is no restriction on the number of nodes that may exist in a single
// program.
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
	debug        bool

	wg              *sync.WaitGroup
	demultiplexQuit chan struct{}
	healthyQuit     chan struct{}
	reconnectQuit   chan struct{}
}

// New creates a new Node that can be used immediately. In order to communicate
// with other nodes, remotes must be added with the Add method.
//
// The local address should be of the form "host:port". If the port is 0,
// then one will be chosen for you automatically. The chosen port can be
// accessed with the Addr method.
func New(laddr string) (*Node, error) {
	return newNode(laddr, false,
		5*time.Minute, 30*time.Second, 10*time.Second)
}

func newNode(
	laddr string,
	debug bool,
	reconnect, healthy, network time.Duration,
) (*Node, error) {
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
		durReconnect: reconnect,
		durHealthy:   healthy,
		durNetwork:   network,
		debug:        debug,

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
		n.kill()
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
func (n *Node) Broadcast(data []byte) {
	n.broadcast(msgUser, data)
}

// Send sends the payload to the specified remote.
func (n *Node) Send(to Remote, data []byte) error {
	if err := n.send(to, msgUser, data); err != nil {
		return err
	}
	return nil
}

// Close gracefully shuts down this node from the cluster and returns only
// after all goroutines associated with the node have stopped.
// Other nodes will not attempt reconnection.
func (n *Node) Close() {
	n.broadcast(msgRemove, nil)
	n.tcp.Close()
}

func (n *Node) kill() {
	close(n.Inbox)
	n.healthyQuit <- struct{}{}
	n.reconnectQuit <- struct{}{}
	n.demultiplexQuit <- struct{}{}

	n.wg.Wait()
	for _, r := range n.Remotes() {
		n.unlearn(r, true)
	}
}

// CloseRemote gracefully closes a connection with the remote specified.
// No automatic reconnection will be made.
func (n *Node) CloseRemote(r Remote) {
	n.send(r, msgRemove, nil)
	n.unlearn(r, true)
}

// Remotes returns a slice of all remotes known by the node.
func (n *Node) Remotes() []Remote {
	n.remlock.RLock()
	defer n.remlock.RUnlock()

	rs := make([]Remote, 0, len(n.remotes))
	for _, r := range n.remotes {
		rs = append(rs, r)
	}
	return rs
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

// SetDebug, when `on` is true, will output more messages to stderr.
func (n *Node) SetDebug(on bool) {
	n.optlock.Lock()
	defer n.optlock.Unlock()

	n.debug = on
}

func (n *Node) getDebug() bool {
	n.optlock.RLock()
	defer n.optlock.RUnlock()

	return n.debug
}
