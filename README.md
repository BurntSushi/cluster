Package cluster provides a small and simple API to manage a set of remote
peers. It falls short of a distributed hash table in that the only
communication allowed between two nodes is direct communication.

The central contribution of this package is to keep the set of remote peers
updated and accurate. Namely, whenever a remote is added, that remote will
share all of the remotes that it knows about. The result is a very simple form
of peer discovery. This also includes handling both graceful and ungraceful
disconnections. In particular, if a node is disconnected ungracefully, other
nodes will periodically try to reconnect with it.

## Installation

go get github.com/BurntSushi/cluster

## Example

Here's a contrived example which creates two nodes and broadcasts a message.

```go
package main

import (
    "fmt"
    "log"
    "github.com/BurntSushi/cluster"
)

func main() {
    // Start two nodes on a randomly chosen port.
    n1, err := cluster.New("localhost:0")
    if err != nil {
        log.Fatal(err)
    }
    
    n2, err := cluster.New("localhost:0")
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
}
```

