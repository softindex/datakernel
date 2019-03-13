## Global-OT

Global-OT is a technology for developing eventually consistent distributed data storages on the scale of the Internet 
with transactional semantics and automatic conflict resolution, using [OT (operational transformation)](https://en.wikipedia.org/wiki/Operational_transformation) 
algorithms. Global-OT provides developers full application stack, with set of specifications and implementations to build 
applications using principles which conceptually resemble Git, but in a more advanced and multi-purpose way. 

### The main features of Global-OT are:
* Data storing and sharing security
    * All data is organized as commit graphs and stored in repositories. They can be  accessed by public key and name.
    * Repositories are securely protected from possible third-party modifications:
    * Only owner of private key can sign and push commits to repository
    * Commits authenticity can be verified by repository public key
    * Commits can optionally be encrypted  
    * Commits are content addressable (commit id is hash of the commit and its parents), which makes entire history of 
    commit graphs immutable.
    
* User-defined data types and working with commit graph of any complexity:
    * Global-OT can work with any user-defined data types. This data is stored in nodes of commit graphs. The edges of 
    commit graphs store user-defined operations which can be applied to the data.
    * Global-OT uses user-defined OT-rules. If you’re not familiar with OT concepts, explore this article. In a nutshell, 
    OT enables to merge conflict operations without data loss, using special rules for merging (see picture 1). 
    * Global-OT framework automatically merges commit graphs of any complexity, using a special algorithm of recursive 
    merging. It splits complex commit graph to atomic OT operations.
    
### Global-OT network design:
Global-OT has a fully decentralized multi-tier network design:
1. Client applications layer - all Global-OT applications, such as mobile and desktop applications or even a server.
2. P2P Global-OT servers layer which can be split in two logical layers:
    * Master OT-Servers (store original file systems)
    * Caching OT-Servers (store cache of file systems)
3. P2P Discovery Service layer - has a DNS-like role, stores information about IPs and public keys of the uploaded 
file systems.

This solution provides the following features:
* There is no need for central lock service or ZooKeeper-like services.
* Global-OT servers can be installed near data consumers (in the same datacenter, on ISP last mile, or even embedded 
directly into application) - which means extremely low latencies and highest data availability.
* Since commit graphs are immutable, Global-OT allows caching and prefetching:
     * Users can connect to any Global-OT servers worldwide to get cached or prefetched commits of any repository 
     * Network scales naturally under load - the more a certain repository is accessed, the more of its cached copies 
are created
* This network design provides high fault tolerance and allows to process concurrent commits from numerous sources 
simultaneously
* All Global-OT servers are interchangeable and application-agnostic (any Global-OT server can work with any Global-OT 
applications or repositories).

### Some possible use cases
Global-OT has a multi-purpose protocol and is not bound to specific tasks. That's why it is applicable to various solutions,
for example:
* Applications for collaborative text editing
* OLAP/OLTP databases with commit semantics
* Instant messenger application
* Global Cloud storage

You can experience how OT works by checking out our [demo Global-OT application](https://github.com/softindex/datakernel/tree/master/examples/global-ot-demo)