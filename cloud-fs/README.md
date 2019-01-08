## FS

FS Module is basis for building efficient, scalable remote file storage. supporting data redundancy, rebalancing and 
resharding. The key principle is effective data immutability, which enables simple design, low-overhead and aggressive 
caching. This technology allows to create high-throughput data storage and processing applications.
All data in FS is organized in file systems.

This module includes `FsClient` interface which represents a client with *upload*, *download*, *move*, *delete* and 
*list* operations. There are several implementations of the interface:

* LocalFsClient - a client which works with local file system and doesn't involve working with networks.
* RemoteFsClient - connects to a RemoteFsServer and communicates with it.
* RemoteFsClusterClient - a client which operates on a map of other clients as a cluster.

Also, there is a `RemoteFsServer` which is an implementation of `AbstractServer` for Cloud-FS.

### You can explore FS examples [here](https://github.com/softindex/datakernel/tree/master/examples/remotefs)
