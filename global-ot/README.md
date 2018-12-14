Global-OT is a technology for developing eventually consistent distributed data storages on the scale of the Internet 
with transactional semantics and automatic conflict resolution, using OT (operational transformation) algorithms. 
Global-OT provides developers full application stack, with set of specifications and implementations to build 
applications using principles which conceptually resemble Git, but more advanced and multi-purpose. 

The main features of Global-OT are:
* Data storing and sharing security
    * All data is organized as commit graphs and stored in repositories. They can be  accessed by public key and name.
    * Repositories are securely protected from possible third-party modifications:
    * Only owner of private key can sign and push commits to repository
    * Commits authenticity can be verified by repository public key
    * Commits can optionally be encrypted  
    * Commits are content addressable (commit id is hash of the commit and its parents), which makes entire history of 
    commit graphs immutable.
    
* Efficient network design
    * Global-OT has a fully decentralized multi-tier network design. There is no need for central lock service or 
    ZooKeeper-like services.
    * Global-OT servers can be installed near data consumers (in the same datacenter, on ISP last mile, or even embedded 
    directly into application) - which means extremely low latencies and highest data availability.
    * Since commit graphs are immutable, Global-OT allows caching and prefetching:
        * Users can connect to any Global-OT servers worldwide to get cached or prefetched commits of any repository 
        * Network scales naturally under load - the more a certain repository is accessed, the more of its cached copies 
        are created
    * This network design provides high fault tolerance and allows to process concurrent commits from numerous sources 
    simultaneously
    * All Global-OT servers are interchangeable and application-agnostic (any Global-OT server can work with any 
    Global-OT applications or repositories).

* User-defined data types and working with commit graph of any complexity:
    * Global-OT can work with any user-defined data types. This data is stored in nodes of commit graphs. The edges of 
    commit graphs store user-defined operations which can be applied to the data.
    * Global-OT uses user-defined OT-rules. If you’re not familiar with OT concepts, explore this article. In a nutshell, 
    OT enables to merge conflict operations without data loss, using special rules for merging (see picture 1). 
    * Global-OT framework automatically merges commit graphs of any complexity, using a special algorithm of recursive 
    merging. It splits complex commit graph to atomic OT operations

You can experience how OT works by checking out our [demo OT application](https://github.com/softindex/datakernel/tree/master/examples/global-ot-demo)