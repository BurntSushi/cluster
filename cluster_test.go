package cluster

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

var (
	flagLocalPort  = 0
	flagRemotePort = 0
	flagRemoteHost = ""
)

func init() {
	flag.IntVar(&flagLocalPort, "lport", flagLocalPort, "Local port")
	flag.IntVar(&flagRemotePort, "rport", flagRemotePort, "Remote port")
	flag.StringVar(&flagRemoteHost, "rhost", flagRemoteHost, "Remote host")
	flag.Parse()
}

func ExampleBroadcast() {
	// Start two nodes on a randomly chosen port.
	n1, err := New("localhost:0")
	if err != nil {
		log.Fatal(err)
	}

	n2, err := New("localhost:0")
	if err != nil {
		log.Fatal(err)
	}

	// Make the two nodes aware of each other.
	if err := n1.Add(n2.Addr().String()); err != nil {
		log.Fatalf("Could not connect node 1 to node 2: %s", err)
	}

	// Wait for the remote to be added and then broadcast
	// a message from node 1.
	n1.RemoteAdded(func(_ Remote) {
		n1.Broadcast([]byte("Hello, world!"))
	})

	// Receive the message in node 2's inbox and print.
	m := <-n2.Inbox
	fmt.Println(string(m.Payload))

	// Output:
	// Hello, world!
}

// Tests ungraceful disconnection by killing the TCP listener for one of
// the nodes. The outcome should be that the other node (eventually) picks
// up on it and unlearns the bad remote.
func TestUngraceful(t *testing.T) {
	if isDebug() {
		return
	}
	n1, n2 := twoNodes(t)

	n1.tcp.Close()
	time.Sleep(6 * time.Second)
	if len(n2.remotes) > 0 {
		t.Fatalf("node '%s' still has remotes: %s", n2, n2.remotes)
	}
}

// Tests that two nodes can send and receive messages to each other.
func TestMessages(t *testing.T) {
	if isDebug() {
		return
	}
	n1, n2 := twoNodes(t)

	n1mesg, n2mesg := "from node1", "from node2"

	n1.Broadcast([]byte(n1mesg))
	n2.Broadcast([]byte(n2mesg))

	m1 := <-n1.Inbox
	m2 := <-n2.Inbox

	if string(m1.Payload) != n2mesg {
		t.Fatalf("node 1 should have received '%s' but got '%s'.",
			n2mesg, string(m1.Payload))
	}
	if string(m2.Payload) != n1mesg {
		t.Fatalf("node 2 should have received '%s' but got '%s'.",
			n1mesg, string(m2.Payload))
	}
}

// Tests the property that closing a remote on one node kills the
// connection on both sides.
func TestGracefulClose(t *testing.T) {
	if isDebug() {
		return
	}
	n1, n2 := twoNodes(t)
	n1.Close()

	time.Sleep(1 * time.Second)
	assertNoRemotes(t, n1)
	assertNoRemotes(t, n2)
}

// Tests the property that closing a remote on one node kills the
// connection on both sides.
func TestGracefulRemoteClose(t *testing.T) {
	if isDebug() {
		return
	}
	n1, n2 := twoNodes(t)

	n1ton2 := n1.Remotes()[0]
	n1.CloseRemote(n1ton2)

	time.Sleep(1 * time.Second)
	assertNoRemotes(t, n1)
	assertNoRemotes(t, n2)
}

// The test fails if either node has any remotes.
// This is intended to test cases when a connection has been closed.
func assertNoRemotes(t *testing.T, n *Node) {
	if len(n.remotes) > 0 {
		t.Fatalf("node '%s' still has remotes: %s", n, n.remotes)
	}
	if len(n.history) > 0 {
		t.Fatalf("node '%s' still has a history of remotes: %s", n, n.history)
	}
}

// twoNodes creates two end points, connects them and waits until each
// has acknowledged the other. Any deviation causes the test to fail.
func twoNodes(t *testing.T) (*Node, *Node) {
	n1, err := newNode("localhost:0", false,
		10*time.Second, 5*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("Could not make first node: %s", err)
	}

	n2, err := newNode("localhost:0", false,
		10*time.Second, 5*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("Could not make second node: %s", err)
	}

	// Use callbacks to tell us when n2 has been added to n1 and when
	// n1 has been added to n2.
	wg := new(sync.WaitGroup)
	wg.Add(2)
	waitForRemote := func(n *Node) func(Remote) {
		return func(r Remote) {
			if r.equal(remote(n.Addr())) {
				wg.Done()
			}
		}
	}
	n1.RemoteAdded(waitForRemote(n2))
	n2.RemoteAdded(waitForRemote(n1))

	if err := n1.Add(n2.Addr().String()); err != nil {
		t.Fatalf("Could not connect node 1 to node 2: %s", err)
	}

	// If the remotes don't sync after 1 second, then assume they never will.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
		// all is well!
	case <-time.After(1 * time.Second):
		t.Fatal("Nodes have not synced after 1 second.")
	}

	// Don't run those callbacks any more.
	n1.RemoteAdded(nil)
	n2.RemoteAdded(nil)
	return n1, n2
}

func isDebug() bool {
	return flagLocalPort > 0 && flagRemotePort > 0 && len(flagRemoteHost) > 0
}

// This is a special debugging test that is run when the right flags are given
// on the command line.
func TestCluster(t *testing.T) {
	if !isDebug() {
		return
	}

	node, err := New(fmt.Sprintf(":%d", flagLocalPort))
	if err != nil {
		t.Fatal(err)
	}
	node.SetDebug(true)

	node.SetReconnectInterval(10 * time.Second)
	node.SetHealthyInterval(10 * time.Second)

	time.Sleep(1 * time.Second)

	raddr := fmt.Sprintf("%s:%d", flagRemoteHost, flagRemotePort)
	if err := node.Add(raddr); err != nil {
		log.Println(err)
	}

	go func() {
		for msg := range node.Inbox {
			fmt.Printf("received: %s\n", string(msg.Payload))
		}
	}()
	node.RemoteAdded(func(r Remote) {
		fmt.Println("Broadcasting...")
		node.Broadcast([]byte("wat wat in the butt. do it in my butt."))
	})

	select {}
}
