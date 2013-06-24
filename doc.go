/*
Package cluster provides a small and simple API to manage a set of remote
peers. It falls short of a distributed hash table in that the only
communication allowed between two nodes is direct communication.

The central contribution of this package is to keep the set of remote peers
updated and accurate. Namely, whenever a remote is added, that remote will
share all of the remotes that it knows about. The result is a very simple form
of peer discovery. This also includes handling both graceful and ungraceful
disconnections. In particular, if a node is disconnected ungracefully, other
nodes will periodically try to reconnect with it.

As of now, there is no standard protocol. Messages are transmitted via GOB
encoding.
*/
package cluster
