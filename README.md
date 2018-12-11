## Introduction

DataKernel is a full-stack application framework for Java. It contains components for building applications of different scales, from single-node HTTP-server to large distributed systems. DataKernel was inspired by Node.js, so it's asynchronous, event-driven, lightweight and very easy to use. Moreover, it is completely free of legacy stuff, such as XML, Java EE, etc.

Due to the usage of modern asynchronous I/O, DataKernel is extremely fast, which is proven by benchmarks.

The essential components of DataKernel form the basis of our ad-serving infrastructure at [AdKernel](http://adkernel.com), running in production environments and processing billions of requests. Specifically, DataKernel is the foundation for systems that provide real-time analytics for ad publishers and advertisers, user data processing tools that we use for ad targeting, and also web crawlers that perform content indexing on a large scale.

## Core components 

### [ByteBuf](http://datakernel.io/docs/modules/bytebuf.html)
ByteBuf is a memory-efficient byte buffer, somewhat similar to Java's  ByteBuffer class. It is useful for fast low-level I/O operations like working with files or transfering data over the internet. Apart from ByteBuf itself, the module contains handy utility classes.

* Represents a wrappage over a byte buffer with separate read and write indices.
* Has a pooling support for better memory efficiency.
* ByteBufQueue class provides effective management of multiple ByteBufs.
* Utility classes manage resizing of underlying byte buffer, String conversions, etc.

[ByteBuf examples](https://github.com/softindex/datakernel/tree/master/examples/bytebuf)

### [Eventloop](http://datakernel.io/docs/modules/eventloop.html)

Eventloop provides efficient non-blocking network and file I/O, for building Node.js-like client/server applications with high performance requirements. It is somewhat similar to Event Loop in Node.js. Although Eventloop runs in a single thread, multiple Eventloops can run at the same time allowing for efficient CPU usage.

* Eventloop utilizes Java's NIO to allow for asynchronous computations and I/O operations.
* Eventloop has an ability to schedule/delay certain tasks for later execution or background execution.
* Because Eventloop is single-threaded it doesn't have to deal with concurrency overhead.

[Eventloop examples](https://github.com/softindex/datakernel/tree/master/examples/eventloop)

### [Net](http://datakernel.io/docs/modules/net.html)

Net module handles low-level asynchronous socket I/O (TCP/UDP). Provides an ability to create servers that run on an eventloop. 

* Support for SSL
* I/O is based on a memory-efficient ByteBufs

[Net examples](https://github.com/softindex/datakernel/tree/master/examples/net)

### [CSP](http://datakernel.io/docs/modules/csp.html)

CSP stands for Communicating Sequential Processes. This module introduces sequential communication between processes via channels, similar to that of the Go Lang. A channel can be seen as a pipe that connects some processes. A value can be sent to a channel via ChannelConsumer and recived from a channel by a ChannelSupplier.

* Support for file I/O using channels.
* Ability to handle exceptions and appropriately close channels, releasing resources and propagating exceptions through communication pipeline.
* Rich DSL that allows to write concise easy-to-read code.

[CSP examples](https://github.com/softindex/datakernel/tree/master/examples/csp)

### [Promise](http://datakernel.io/docs/modules/async.html)

Promise module introduces promises similar to those of JavaScript or to Java's CompletionStage. It is a convenient way to organize asynchronous code. 

* Compared to JavaScript, these promises are heavily optimized - intermediate promises are stateless and the promise graph executes with minimal garbage and overhead.
* Because Eventloop is single-threaded, so are promises. That is why promises are much more efficient comparing to Java's CompletableFutures.
* Utility classes that help to collect results of promises, add loops and conditional logic to promises execution.

[Promise examples](https://github.com/softindex/datakernel/tree/master/examples/promise)

### [Codegen](http://datakernel.io/docs/modules/codegen.html)

Dynamic class and method bytecode generator on top of ObjectWeb ASM. An expression-based fluent API abstracts the complexity of direct bytecode manipulation.

* Bytecode generation ensures that the business logic is as fast as possible.
* Expression-based API allows to write concise instructions to generate bytecode.
* Implements basic relational algebra operations.

[Codegen examples](https://github.com/softindex/datakernel/tree/master/examples/codegen)

### [Serializer](http://datakernel.io/docs/modules/serializer.html)

Extremely fast and space-efficient serializers, crafted using bytecode engineering. Introduces schema-less approach for maximum performance.

* Compatible with dynamically created classes.
* Serialization controlled via annotations.
* No overhead in typed values.

[Serializer examples](https://github.com/softindex/datakernel/tree/master/examples/serializer)  
[Benchmark](http://datakernel.io/docs/modules/serializer.html#benchmark)

### [Datastream](http://datakernel.io/docs/modules/streams.html)

Composable asynchronous/reactive streams with powerful data processing capabilities. Useful for transfering high-volumes of lightweight values.

* Stream based I/O on top of an Eventloop module.
* Support for composable stream operations (mappers, reducers, splitters, serialization).
* Compatibility with CSP module.

[Datastream examples](https://github.com/softindex/datakernel/tree/master/examples/datastreams)  
[Benchmark](http://datakernel.io/docs/modules/streams.html#benchmark)

### [Http](http://datakernel.io/docs/modules/http.html)

High-performance asynchronous HTTP client and server. Contains a built-in servlets for request dispatching, loading of a static content.

* GC pressure is low because HTTP connections are managed in a pool and recyclable ByteBufs are used to wrap HTTP messages.
* Supports requests with streaming request body (using CSP).
* Contains a DNS client that can be used to cache results of a DNS queries.
* AsyncServlet represents a functional interface which allows for easy custom servlet creation without overhead.

[Http examples](https://github.com/softindex/datakernel/tree/master/examples/http)  
[Benchmark](http://datakernel.io/docs/modules/http.html#benchmark)

### [JSON](http://datakernel.io/docs/modules/json.html)

Contains tools for encoding/decoding of primitives and objects. The process is somewhat similar to serialization. 

* Can be used to convert objects to a custom representation.
* Built-in support for conversion to JSON and ByteBuf.
* Codec registry allows to easily access pre-defined codecs.

[JSON examples](https://github.com/softindex/datakernel/tree/master/examples/codec)  

## Cloud components

### [RPC](http://datakernel.io/docs/modules/rpc.html)

High-performance and fault-tolerant remote procedure call module for building distributed applications. Provides a high-performance asynchronous binary RPC streaming protocol.

* Contains consistent distribution strategies.
* Utilizes Serialization module for fast RPC message transfering.
* Fault tolerance is achieved by reconnections to fallback and replica servers

[RPC examples](https://github.com/softindex/datakernel/tree/master/examples/rpc)  

### [FS](http://datakernel.io/docs/modules/remotefs.html)

Basis for building efficient, scalable remote file servers and clients. Utilizes CSP for fast and reliable file transfer.

* Ability to create clustered network of file servers.
* Fast asynchronous file I/O based on Java's NIO.
* Ability to cache downloaded files.

[FS examples](https://github.com/softindex/datakernel/tree/master/examples/remotefs)  

### [OT](http://datakernel.io/docs/modules/ot.html)

Implementation of operational transformation technology which allows to build collaborative software systems. Represents a graph of commits where each commit contains some number of operations. To create an OT system, user has to provide a set of rules for handling transformation, conflict resolution, etc.

* Structure is somewhat similar to Git.
* Efficient algorithms for managing graph of commits.
* Ability to save/load snapshots and create backups.
* State manager allows to monitor current state of an OT system.

### [OT-MySql](http://datakernel.io/docs/modules/ot-mysql.html)

A binding of an OT repository to a MySql database. Introduces a persistant OT commit storage, useful for managing high loads of commits.

* Although, here MySql is used as a database of choice, nothing stops you from implementing `OTRepository` interface based on any other database. You can even store commits in-memory or as files on a disk.

### [LSMT Table](http://datakernel.io/docs/modules/aggregation.html)

Log-structured merge-tree table with fields representing aggregate functions, designed for OLAP workload. Provides you with an ability to build almost any kind of database by defining your own aggregation functions.

* Utilizes log-structured merge-tree data structure, so databases built on top of this table can easily handle high insert volume data (e.g. transactional logs).

### [LSMT Database](http://datakernel.io/docs/modules/cube.html)

Multidimensional OLAP (Online Analytical Processing) database with predefined set of dimensions, measures, and log-structured merge-tree tables containing pre-aggregated data. LSMT database efficiently executes multi-dimensional analytical queries. Dimension here can be seen as a category while a measure is a value of some kind. 

* Utilizes FS and OT modules which allows database to be truly asynchronous and distributed.

### [Dataflow](http://datakernel.io/docs/modules/datagraph.html)

Distributed stream-based batch processing engine for Big Data applications. Represents a set of tools to work with datasets which can span multiple partitions. User can write a tasks to be executed on a dataset. The task is then compiled into an execution graphs and passed as a JSON commands to a corresponding worker servers to be executed.

* Data graph consists of nodes that correspond to certain operations (e.g. Download, Filter, Map, Reduce, Sort, etc).
* User can define custom predicates, mapping functions, reducers to be executed on datasets.
* Nodes in a data graph have inputs and outputs which are identified by a unique StreamId. This allows for an inter-partition data computation.
* Since nodes are stateless by itself, computation is similar to passing data items through a pipeline, applying certain operaions in the process.

### [CRDT](http://datakernel.io/docs/modules/crdt.html)

Conflict-free replicated data type implementation. More specifically, implementation of state-based CRDT. Contains tools to create collaborative editing applications using CRDT approach. 

* Automatically resolves all conflicts that arise with concurrent data manipulation.
* Merges data that comes from multiple nodes into a single CRDT structure.

[CRDT examples](https://github.com/softindex/datakernel/tree/master/examples/crdt)
### ETL
// TODO eduard: add description

### Multilog
// TODO eduard: add description

## Global components

### Global Common

A foundation for other global componenents. Contains a `DiscoveryService` that is like a DNS for other global nodes. This module also contains cryptography tools and some common classes. Cryptography allows nodes to work with non-trusted servers. 

### [Global FS](http://datakernel.io/docs/modules/global-fs.html)

A distributed file system that is an alternative for IPFS and BitTorrent.

* Fault tolerance let's you work with local global fs node if you do not have current connection to a master server. Changes made locally will be pushed to a master node as soon as connection is re-established.
* A global fs node can work in proxy mode (as a router), in caching mode (locally saving files that it downloads/uploads) or in pre-fetching mode (uploading files from master nodes in advance).
* Data transferred between nodes is cryptographically signed, so it cannot be tempered with.

[Global FS Demo](https://github.com/softindex/datakernel/tree/master/examples/global-fs-demo)

### [Global OT](http://datakernel.io/docs/modules/global-ot.html)

Represents a distributed Git-like graph of immutable content-addressable commits, encrypted and signed by data owner. It allows to save data 

* Fault tolerance let's you work with local global ot node if you do not have current connection to a master server. Changes made locally will be pushed to a master node as soon as connection is re-established.
* A global OT node can work in proxy mode (as a router), in caching mode (locally saving commits that it loads/pushes) or in pre-fetching mode (uploading commits from master nodes in advance).

[Global OT Demo](https://github.com/softindex/datakernel/tree/master/examples/global-ot-demo)

## Integration components

### [Boot](http://datakernel.io/docs/modules/boot.html)

An intelligent way of booting complex applications and services according to their dependencies. Component contains several modules that help to easily configure and launch an application. The main component is a `ServiceGraphModule` which builds dependency graph based on Guice's object graph. It is used to start or stop services concurrently, according to their dependencies. 

* Although, Guice is used here as a dependency injector tool, it is not the main component of a module and can be swapped out with some other tool.
* `ConfigModule` helps to configure services in a simple way. There is a rich set of config converters that can be used to set up your application.
* `WorkerPoolModule` introduces a worker pool, which is a container for other services. It is an easy way to utilize all of available cores by running a worker `Eventloop` on each core.
* `JmxModule` enables tools to monitor a componenent's lifecycle via `JmxAttribute`s or even interfer with it by the means of `JmxOperation`s
* `TriggersModule` adds the ability to place certain triggers on a module that will fire as soon as some condition fullfills. These triggers can be monitored via JMX.
* Boot module introduces a concept of a `Launcher`, which can be seen as a basic application. It uses `ServiceGraph` to properly boot all of provided dependencies.

[Boot examples](https://github.com/softindex/datakernel/tree/master/examples/boot)

### [Launchers](http://datakernel.io/docs/modules/launchers.html)

Module contains a set of predefined launchers as well as some common initializers for services. When a user wants to create his own application, he should either extend one of proposed launchers or use it as a reference for his custom launcher.

Contains standard launchers for:  
* CRDT module
* HTTP module
* FS module
* RPC module

[Launchers examples](https://github.com/softindex/datakernel/tree/master/examples/launchers)

### [UIKernel](http://datakernel.io/docs/modules/uikernel.html)
This module represents a integration with UIKernel.io JS frontend library: JSON serializers, grid model, basic servlets. Using this module you can build a server application that will be compatible with UIKernel JS library. 

[UIKernel integration example](https://github.com/softindex/datakernel/tree/master/examples/uikernel-integration)
