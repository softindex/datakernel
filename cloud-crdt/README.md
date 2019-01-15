## CRDT

CRDT module was designed to create collaborative editing applications with CRDT (conflict-free replicated data type) 
approach. Well suitable for simple solutions (for example, a scalable eventually consistent key-value storage 
with CRDT conflict resolutions).

This module includes implementations of CRDT Client and Server:
* `CrdtClient` - an interface for various CRDT Client implementations.
* `CrdtClusterClient` - an implementation of `CrdtClient`.
* `RemoteCrdtClient` - an implementation of `CrdtClient`.
* `RuntimeCrdtClient` - an implementation of `CrdtClient`.
* `FsCrdtClient` - a CRDT client for working with Cloud-FS.
* `RocksDBCrdtClient` - a CRDT client for working with RocksDB.
* `CrdtServer` - a CRDT server which processes uploads, downloads and removes of data.

There are also some useful builtin primitives needed for convenient CRDT applications development:

* `GCounterInt` - an increment-only counter of int type.
* `GCounterLong` - an increment-only counter of long type.
* `GSet` - a grow-only set.
* `TPSet` - a set which supports both adds and removes.
* `LWWSet`- a Last Write Wins set.
* `PNCounterInt` - an counter of `int` type which supports both increments and decrements.
* `PNCounterLong` - a counter of `long` type which supports both increments and decrements.

### You can explore CRDT example [here](https://github.com/softindex/datakernel/blob/master/examples/crdt)

