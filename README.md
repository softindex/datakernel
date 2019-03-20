<img src="http://datakernel.io/static/images/logo.png">

## Introduction

DataKernel is a full-stack application framework for Java. 
It contains components for building **application-specific embedded databases, specialized high-performance servers, clients and network protocols** 
and **applications of different scales**: from single-node HTTP-server to large distributed systems spanning multiple data centers. 

DataKernel framework is **asynchronous, event-driven, lightweight and legacy-free**, featuring modern **high-level abstractions** and **DSLs**. 
Its asynchronous architecture was **inspired by Node.js**, and efficient multi-threading model makes DataKernel **extremely fast**, which is proven by various benchmarks.

### The main concepts of DataKernel are:
1. Fully **decentralized and asynchronous** cluster architecture with no centralized Apache ZooKeeper-like consensus algorithms.
2. Developing **embedded servers and services** that are tailored to specific business application instead of "config file"-configured servers. 
3. Focus on **function composition and OOP design** to develop complicated services from simple building blocks and strategies.
4. No multi-threading overhead. All worker threads are essentially **single-threaded** but can interact with each other and common threadsafe shared state. 
5. Modern and efficient solutions as core technologies:
    * Optimized **Eventloop and Promise**, inspired by Node.js
    * **[CSP](https://en.wikipedia.org/wiki/Communicating_sequential_processes)** implementation, similar to Go language
    * **Bytecode generation** - for high-performance data serializers and for runtime data processing
    * Fast and high-level network abstractions
6. Incorporating **[LSMT](https://en.wikipedia.org/wiki/Log-structured_merge-tree), 
[CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) and 
[Operational Transformation (OT)](https://en.wikipedia.org/wiki/Operational_transformation)** storage engines and algorithms, 
with built-in support of **distributed data storage and processing**. 

DataKernel is an actively used production-ready technology. It has a full **JMX monitoring** coverage of all of the 
components and also an **automatic bootstrap of applications' components graph**. It also has a high-performance embedded HTTP 
server for developing of full-stack web applications **with support of React.js front-end** and **minimalistic Dependency Injection.**

The essential components of DataKernel form the basis of diverse high-load applications processing billions of requests 
daily: ad-serving solutions, online analytics and web crawlers that perform content indexing on a large scale.

## Getting started

You can start with ["Hello World!"](https://github.com/softindex/datakernel/tree/master/examples/getting-started) 5-minute guide, which 
shows how to create a simple “Hello World” application using Eventloop - one of the core components of DataKernel framework.

If you are interested in developing web-applications with JavaScript front-end, explore
[UIKernel integration](https://github.com/softindex/datakernel/tree/master/examples/uikernel-integration) example.

Also, check out [examples module](https://github.com/softindex/datakernel/tree/master/examples), which includes examples 
for most of the DataKernel components along with use cases of their combinations.

## DataKernel framework structure

DataKernel framework consists of 30+ modules of different complexity and purposes. All of the components can be 
logically split in 4 groups:

* [Core components](#core-components) - building blocks of DataKernel framework and some core technologies
* [Cloud components](#cloud-components) - modules designed for cloud solutions
* [Global components](#global-components) - technologies for creating global network applications
* [Integration components](#integration-components) - include modules for DataKernel components integration and 
integration with UIKernel.io front-end JS library.

### Core components
| Core components | | |
|---|---|---|
| **[ByteBuf](https://github.com/softindex/datakernel/tree/master/core-bytebuf)** | ByteBuf is a more lightweight and efficient version of Java's `ByteBuffer` class with support of simultaneous input and output. Module's `ByteBufQueue` allows you to work with multiple `ByteBuf`s in a convenient way. `ByteBufPool` significantly reduces memory consumption and Java Garbage Collector load by reusing `ByteBuf`s. You can also get stats on pool's memory consumption and efficiency. | [ByteBuf examples](https://github.com/softindex/datakernel/tree/master/examples/bytebuf) |
| **[Eventloop](https://github.com/softindex/datakernel/tree/master/core-eventloop)** | Eventloop resembles Event Loop in Node.js and provides efficient non-blocking network and file I/O for building Node.js-like client/server applications with high performance requirements. Although Eventloop runs in a single thread, multiple Eventloops can run at the same time to provide efficient CPU usage. | [Eventloop examples](https://github.com/softindex/datakernel/tree/master/examples/eventloop) |
| **[Net](https://github.com/softindex/datakernel/tree/master/core-net)** | Net module handles low-level asynchronous socket I/O (TCP/UDP) based on ByteBuf. Provides ability to create servers that utilize Eventloop and support SSL. | [Net examples](https://github.com/softindex/datakernel/tree/master/examples/net) |
| **[CSP](https://github.com/softindex/datakernel/tree/master/core-csp)** | CSP stands for Communicating Sequential Processes. This module provides asynchronous sequential communication between processes via channels similarly to the Go language. | [CSP examples](https://github.com/softindex/datakernel/tree/master/examples/csp) |
| **[Promise](https://github.com/softindex/datakernel/tree/master/core-promise)** | Promise module resembles JavaScript Promise and is an alternative to Java's `CompletionStage`. It allows to organize asynchronous code in convenient way by creating chains of operations which are wrapped in `Promise`s. They have a minimalistic and extremely fast single-threaded implementation with minimal overhead and memory consumption. | [Promise examples](https://github.com/softindex/datakernel/tree/master/examples/promise) |
| **[Codegen](https://github.com/softindex/datakernel/tree/master/core-codegen)** | Dynamic class and method bytecode generator on top of ObjectWeb ASM. An expression-based fluent API abstracts the complexity of direct bytecode manipulation, so you can use it to create custom classes on the fly by providing a class description. It is particularly useful in the situations when you have to adjust class behavior in the runtime, based on some external factors. | [Codegen examples](https://github.com/softindex/datakernel/tree/master/examples/codegen) |
| **[Serializer](https://github.com/softindex/datakernel/tree/master/core-serializer)** | Extremely fast and space-efficient serializers created with bytecode engineering. Introduces schema-less approach for best performance. | [Serializer examples](https://github.com/softindex/datakernel/tree/master/examples/serializer), [Benchmark](https://github.com/softindex/datakernel/tree/master/core-serializer#benchmark) |
| **[Datastream](https://github.com/softindex/datakernel/tree/master/core-datastream)** | Composable asynchronous/reactive streams with powerful data processing capabilities. Useful for transferring high volumes of lightweight values. | [Datastream examples](https://github.com/softindex/datakernel/tree/master/examples/datastreams), [Benchmark](https://github.com/softindex/datakernel/tree/master/core-datastream#benchmark)|
| **[HTTP](https://github.com/softindex/datakernel/tree/master/core-http)** | High-performance asynchronous HTTP client and server. Contains built-in servlets for request dispatching and static content loading. | [HTTP examples](https://github.com/softindex/datakernel/tree/master/examples/http), [Benchmark](https://github.com/softindex/datakernel/tree/master/core-http#benchmark) |
| **[Codec](https://github.com/softindex/datakernel/tree/master/core-codec)** | Contains tools for encoding/decoding primitives and objects with built-in support of conversion to/from JSON and ByteBuf. The process resembles serialization and can be used to convert objects to a custom representation. | [Codec examples](https://github.com/softindex/datakernel/tree/master/examples/codec) |

### Cloud components 
| Cloud components | | |
| --- | --- | --- |
|**[RPC](https://github.com/softindex/datakernel/tree/master/cloud-rpc)**| High-performance and fault-tolerant remote procedure call module for building distributed applications with an extremely efficient asynchronous binary RPC streaming protocol. | [RPC examples](https://github.com/softindex/datakernel/tree/master/examples/rpc) |
|**[FS](https://github.com/softindex/datakernel/tree/master/cloud-fs)**| Basis for building efficient, scalable remote file storage with implementation of caching and fast asynchronous file I/O based on Java NIO. Utilizes CSP for fast and reliable file transfer. | [FS examples](https://github.com/softindex/datakernel/tree/master/examples/remotefs) |
|**[OT](https://github.com/softindex/datakernel/tree/master/cloud-ot)**| This module allows to build collaborative software systems based on Git-like approach and with automatic conflict resolution, utilizing a special algorithm for operational transformations.||
|**[OT-MySQL](https://github.com/softindex/datakernel/tree/master/cloud-ot-mysql)**| Enables binding OT repositories to MySQL (or any other) database. You can even store `OTRepository` commits in-memory or as files on a disk.||
|**[LSMT Aggregation](https://github.com/softindex/datakernel/tree/master/cloud-lsmt-aggregation)**| Log-structured merge-tree table which stores aggregate functions and designed for OLAP workload.||
|**[LSMT OLAP Cube](https://github.com/softindex/datakernel/tree/master/cloud-lsmt-cube)**|Multidimensional OLAP (Online Analytical Processing) database with predefined set of dimensions, measures, and log-structured merge-tree tables containing pre-aggregated data. LSMT database efficiently executes multi-dimensional analytical queries.||
|**[Dataflow](https://github.com/softindex/datakernel/tree/master/cloud-dataflow)**| Distributed stream-based batch processing engine for Big Data applications. Represents a set of tools to work with data sets which can span multiple partitions. ||
|**[CRDT](https://github.com/softindex/datakernel/tree/master/cloud-crdt)**| Conflict-free replicated data type implementation (specifically, state-based CRDT). Contains tools to create collaborative editing applications using CRDT approach to merge data that comes from multiple nodes into a single CRDT structure.| [CRDT examples](https://github.com/softindex/datakernel/tree/master/examples/crdt) |
|**[Multilog](https://github.com/softindex/datakernel/tree/master/cloud-multilog)**| This module manages integrity of log files stored in distributed file system and allows to work with them as if they were stored in a single place.||
|**[ETL](https://github.com/softindex/datakernel/tree/master/cloud-etl)**| Processes logs using operational transformations. Uses OT module to persist logs and resolve conflicts. ||

### Global components

Global components are designed to be used distributively across the whole global network. Global network consists of 
interconnected nodes (or servers) and discovery services that help to route one node with another. To achieve this and 
preserve data integrity between global nodes, all of the components have the following features:

* Fault tolerance. This allows you to work with local node even in disconnected mode.
* Cryptography. Data transferred between nodes is cryptographically signed, so it cannot be tempered by third parties.
* Different operating modes. A global node can work in proxy mode (as a router), in caching mode (locally saving data 
that it downloads/uploads) or in pre-fetching mode (uploading data from master nodes in advance).

| Global components |||
|---|---|---|
|**[Global Common](https://github.com/softindex/datakernel/tree/master/global-common)**| A foundation for other global components. Contains a `DiscoveryService` that conducts a DNS-like role for other global nodes. This module also contains cryptography tools and some common classes. Cryptography allows nodes to work with non-trusted servers. ||
|**[Global-FS](https://github.com/softindex/datakernel/tree/master/global-fs)**| Allows to create file sharing systems alternative to IPFS / BitTorrent technologies. It is fault tolerant, distributed and can work with non-trusted servers due to implementation of cryptographic data authenticity.|[Global-FS CLI](https://github.com/softindex/datakernel/tree/master/examples/global-fs-cli), [Global-FS Demo](https://github.com/softindex/datakernel/tree/master/examples/global-fs-demo)|
|**[Global-OT](https://github.com/softindex/datakernel/tree/master/global-ot)**| Extends both OT and Blockchain technologies (representing the data as Git-like graph of individual immutable content-addressable commits, encrypted and signed with private key of its owner). Global-OT can work with non-trusted servers. | [Global-OT Demo](https://github.com/softindex/datakernel/tree/master/examples/global-ot-demo), [Global-OT Editor](https://github.com/softindex/datakernel/tree/master/examples/global-ot-editor), [Global-OT Chat](https://github.com/softindex/datakernel/tree/master/examples/global-ot-chat)|
|**[Global-DB](https://github.com/softindex/datakernel/tree/master/global-db)**| This module is a variation of Global-FS, optimized for storing small binary key-value pairs and their synchronization. | [Global-DB Demo](https://github.com/softindex/datakernel/tree/master/examples/global-db-demo)|

### Integration components
|Integration components|||
|---|---|---|
|**[Boot](https://github.com/softindex/datakernel/tree/master/boot)**|An intelligent way of booting complex applications and services according to their dependencies. Component contains several modules that help to configure and launch an application easier.| [Boot examples](https://github.com/softindex/datakernel/tree/master/examples/boot) |
|**[Launchers](https://github.com/softindex/datakernel/tree/master/launchers)**|Module contains a set of predefined launchers as well as some common initializers for services. Contains standard launchers for CRDT, HTTP, FS and RPC modules.| [Launchers examples](https://github.com/softindex/datakernel/tree/master/examples/launchers) |
|**[UIKernel](https://github.com/softindex/datakernel/tree/master/uikernel)**|This module represents integration with UIKernel.io JS frontend library: JSON serializers, grid model, basic servlets. With this module you can build a server application that will be compatible with UIKernel JS library. |[UIKernel integration example](https://github.com/softindex/datakernel/tree/master/examples/uikernel-integration)|
