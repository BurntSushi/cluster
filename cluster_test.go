package cluster

import (
	"flag"
	"fmt"
	"log"
	"testing"
	"time"
)

var (
	flagLocalPort = 0
	flagRemoteHost = ""
	flagRemotePort = 0
)

func init() {
	flag.IntVar(&flagLocalPort, "lport", flagLocalPort, "Local port")
	flag.StringVar(&flagRemoteHost, "rhost", flagRemoteHost, "Remote host")
	flag.IntVar(&flagRemotePort, "rport", flagRemotePort, "Remote port")
	flag.Parse()
}

func TestCluster(t *testing.T) {
	if flagLocalPort == 0 || len(flagRemoteHost) == 0 || flagRemotePort == 0 {
		return
	}

	c, err := New(fmt.Sprintf("localhost:%d", flagLocalPort))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	raddr := fmt.Sprintf("%s:%d", flagRemoteHost, flagRemotePort)
	if err := c.Add(raddr); err != nil {
		log.Println(err)
	}

	c.broadcast(mesg(msgUser, "wat wat in the butt"))
	m := <-c.Inbox

	if m == nil {
		t.Fatal("BAH")
	}
	fmt.Printf("received: %s\n", string(m.Payload))

	select {}
}

func oneRemote(c *Cluster) *Remote {
	for _, r := range c.remotes {
		return r
	}
	return nil
}
