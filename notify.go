package cluster

import (
	"sync"
)

type callbacks struct {
	lock          *sync.Mutex
	remoteAdded   func(Remote)
	remoteRemoved func(Remote)
	remoteChanged func([]Remote)
}

func newCallbacks() *callbacks {
	return &callbacks{
		lock:          new(sync.Mutex),
		remoteAdded:   nil,
		remoteRemoved: nil,
		remoteChanged: nil,
	}
}

func (cbs *callbacks) locked(f func()) {
	cbs.lock.Lock()
	defer cbs.lock.Unlock()
	f()
}

func (n *Node) RemoteAdded(f func(r Remote)) {
	n.notify.locked(func() {
		n.notify.remoteAdded = f
	})
}

func (n *Node) runRemoteAdded(r Remote) {
	go n.notify.locked(func() {
		if n.notify.remoteAdded != nil {
			n.notify.remoteAdded(r)
		}
	})
}

func (n *Node) RemoteRemoved(f func(r Remote)) {
	n.notify.locked(func() {
		n.notify.remoteRemoved = f
	})
}

func (n *Node) runRemoteRemoved(r Remote) {
	go n.notify.locked(func() {
		if n.notify.remoteRemoved != nil {
			n.notify.remoteRemoved(r)
		}
	})
}

func (n *Node) RemoteChanged(f func(rs []Remote)) {
	n.notify.locked(func() {
		n.notify.remoteChanged = f
	})
}

func (n *Node) runRemoteChanged() {
	go n.notify.locked(func() {
		if n.notify.remoteChanged != nil {
			n.remlock.RLock()
			rs := make([]Remote, 0, len(n.remotes))
			for _, r := range n.remotes {
				rs = append(rs, r)
			}
			n.remlock.RUnlock()

			n.notify.remoteChanged(rs)
		}
	})
}
