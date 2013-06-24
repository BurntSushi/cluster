package cluster

import (
	"flag"
	"fmt"
	"log"
	"testing"
	"time"
)

var (
	flagLocalPort  = 0
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

	node, err := New(fmt.Sprintf("localhost:%d", flagLocalPort))
	if err != nil {
		t.Fatal(err)
	}

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
