package cluster

import (
	"fmt"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	c1, err := New("localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	c2, err := New("localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	if err := c1.Add(fmt.Sprintf("localhost:%d", c2.Addr().Port)); err != nil {
		t.Fatal(err)
	}
	if err := c2.Add(fmt.Sprintf("localhost:%d", c1.Addr().Port)); err != nil {
		t.Fatal(err)
	}

	r1 := oneRemote(c1)
	r2 := oneRemote(c2)

	go func() {
		r1.send <- mesg(msgUser, "cauchy")
	}()
	go func() {
		r2.send <- mesg(msgUser, "plato")
	}()

	m1 := <-r1.recv
	m2 := <-r2.recv

	fmt.Printf("c1 received: %s\n", string(m1.Payload))
	fmt.Printf("c2 received: %s\n", string(m2.Payload))

	select {}
}

func oneRemote(c *Cluster) *Remote {
	for _, r := range c.remotes {
		return r
	}
	return nil
}
