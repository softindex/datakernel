## Introduction

DataKernel is a full-stack application framework for Java. It contains components for building applications of different 
scales, from single-node HTTP-server to large distributed systems. DataKernel was inspired by Node.js, so it's 
asynchronous, event-driven, lightweight and very easy to use. Moreover, it is completely free of legacy stuff, such 
as XML, Java EE, etc.

Due to the usage of modern asynchronous I/O, DataKernel is extremely fast, which is proven by benchmarks.

The essential components of DataKernel form the basis of high-load applications running in production environments and 
processing billions of requests. Specifically, DataKernel is the foundation for systems that provide real-time analytics, 
user data processing tools and also web crawlers that perform content indexing on a large scale.

## Core components 

### [ByteBuf](https://github.com/softindex/datakernel/tree/master/core-bytebuf)
ByteBuf is a more lightweight and efficient version of Java's `ByteBuffer` class with support of simultaneous input and 
output. Module's `ByteBufQueue` allows you to work with multiple `ByteBuf`s in a convenient way. `ByteBufPool` 
significantly reduces memory consumption and Java Garbage Collector load by reusing `ByteBuf`s. It provides you with 
stats on pool's memory consumption and efficiency. 

[ByteBuf examples](https://github.com/softindex/datakernel/tree/master/examples/bytebuf)

### [Eventloop](https://github.com/softindex/datakernel/tree/master/core-eventloop)

Eventloop provides efficient non-blocking network and file I/O for building Node.js-like client/server applications 
with high performance requirements. It resembles Event Loop in Node.js. Although Eventloop runs in a single thread, 
multiple Eventloops can run at the same time providing efficient CPU usage.

[Eventloop examples](https://github.com/softindex/datakernel/tree/master/examples/eventloop)

### [Net](https://github.com/softindex/datakernel/tree/master/core-net)

Net module handles low-level asynchronous socket I/O (TCP/UDP) based on ByteBuf. Provides ability to create servers that 
utilize Eventloop and support SSL.

[Net examples](https://github.com/softindex/datakernel/tree/master/examples/net)

### [CSP](https://github.com/softindex/datakernel/tree/master/core-csp)

CSP stands for Communicating Sequential Processes. This module provides sequential communication between processes via 
channels similarly to the Go language.

[CSP examples](https://github.com/softindex/datakernel/tree/master/examples/csp)

### [Promise](https://github.com/softindex/datakernel/tree/master/core-promise)

Promise module is similar to JavaScript Promise and is alternative to Java's `CompletionStage` and allows to organize 
asynchronous code in convenient way. 

[Promise examples](https://github.com/softindex/datakernel/tree/master/examples/promise)

### [Codegen](https://github.com/softindex/datakernel/tree/master/core-codegen)

Dynamic class and method bytecode generator on top of ObjectWeb ASM. An expression-based fluent API abstracts the 
complexity of direct bytecode manipulation, so you can use it to create custom classes on the fly by providing a class 
description. It is particularly useful in the situations when you have to adjust class behavior in the runtime, based on some external
factors.

[Codegen examples](https://github.com/softindex/datakernel/tree/master/examples/codegen)

### [Serializer](https://github.com/softindex/datakernel/tree/master/core-serializer)

Extremely fast and space-efficient serializers created using bytecode engineering. Introduces schema-less approach for 
best performance.

[Serializer examples](https://github.com/softindex/datakernel/tree/master/examples/serializer)  
[Benchmark](https://github.com/softindex/datakernel/tree/master/core-serializer#benchmark)

### [Datastream](https://github.com/softindex/datakernel/tree/master/core-datastream)

Composable asynchronous/reactive streams with powerful data processing capabilities. Useful for transferring high volumes 
of lightweight values.

[Datastream examples](https://github.com/softindex/datakernel/tree/master/examples/datastreams)  
[Benchmark](https://github.com/softindex/datakernel/tree/master/core-datastream#benchmark)

### [HTTP](https://github.com/softindex/datakernel/tree/master/core-http)

High-performance asynchronous HTTP client and server. Contains a built-in servlets for request dispatching and static 
content loading.  

[HTTP examples](https://github.com/softindex/datakernel/tree/master/examples/http)  
[Benchmark](https://github.com/softindex/datakernel/tree/master/core-http#benchmark)

### [JSON](https://github.com/softindex/datakernel/tree/master/core-json)

Contains tools for encoding/decoding primitives and objects with built-in support of conversion to/from JSON and ByteBuf. 
The process resembles serialization and can be used to convert objects to a custom representation.

[JSON examples](https://github.com/softindex/datakernel/tree/master/examples/codec)  

## Cloud components

### [RPC](https://github.com/softindex/datakernel/tree/master/cloud-rpc)

High-performance and fault-tolerant remote procedure call module for building distributed applications with an extremely
efficient asynchronous binary RPC streaming protocol.

[RPC examples](https://github.com/softindex/datakernel/tree/master/examples/rpc)  

### [FS](https://github.com/softindex/datakernel/tree/master/cloud-fs)

Basis for building efficient, scalable remote file storage with implementation of caching and fast asynchronous file I/O 
based on Java NIO. Utilizes CSP for fast and reliable file transfer.

[FS examples](https://github.com/softindex/datakernel/tree/master/examples/remotefs)  

### [OT](https://github.com/softindex/datakernel/tree/master/cloud-ot)

This module allows to build collaborative software systems based on Git-like approach and with automatic conflict 
resolution utilizing a special algorithm for operational transformations.

### [OT-MySQL](https://github.com/softindex/datakernel/tree/master/cloud-ot-mysql)

Enables binding OT repositories to MySQL (or any other) database. You can even store `OTRepository` commits in-memory or 
as files on a disk.

### [LSMT Aggregation](https://github.com/softindex/datakernel/tree/master/cloud-lsmt-aggregation)

Log-structured merge-tree table which stores aggregate functions and designed for OLAP workload.

### [LSMT OLAP Cube](https://github.com/softindex/datakernel/tree/master/cloud-lsmt-cube)

Multidimensional OLAP (Online Analytical Processing) database with predefined set of dimensions, measures, and 
log-structured merge-tree tables containing pre-aggregated data. LSMT database efficiently executes multi-dimensional 
analytical queries.

### [Dataflow](https://github.com/softindex/datakernel/tree/master/cloud-dataflow)

Distributed stream-based batch processing engine for Big Data applications. Represents a set of tools to work with 
data sets which can span multiple partitions.

### [CRDT](https://github.com/softindex/datakernel/tree/master/cloud-crdt)

Conflict-free replicated data type (specifically, state-based CRDT) implementation. Contains tools to create 
collaborative editing applications using CRDT approach merging data that comes from multiple nodes into a single CRDT 
structure. 

[CRDT examples](https://github.com/softindex/datakernel/tree/master/examples/crdt)

### [Multilog](https://github.com/softindex/datakernel/tree/master/cloud-multilog)

This module manages integrity of log files stored in distributed file system and allows to work with them as if they 
were stored in a single place.

### [ETL](https://github.com/softindex/datakernel/tree/master/cloud-etl)

Processes logs using operational transformations. Uses OT module to persist logs and resolve conflicts.

## Global components

Global components are designed to be used distributively across the whole global network. Global network consists of 
interconnected nodes (or servers) and discovery services that help route one node with another. To achieve this and 
preserve data integrity between global nodes, all the components have the following features:

* Fault tolerance. This allows you to work with local node in disconnected mode.
* Cryptography. Data transferred between nodes is cryptographically signed, so it cannot be tempered by third parties.
* Different operating modes. A global node can work in proxy mode (as a router), in caching mode (locally saving data 
that it downloads/uploads) or in pre-fetching mode (uploading data from master nodes in advance).

### [Global Common](https://github.com/softindex/datakernel/tree/master/global-common)

A foundation for other global components. Contains a `DiscoveryService` that is like a DNS for other global nodes. This 
module also contains cryptography tools and some common classes. Cryptography allows nodes to work with non-trusted 
servers. 

### [Global-FS](https://github.com/softindex/datakernel/tree/master/global-fs)

A framework to create file sharing systems alternative to IPFS / BitTorrent technologies. It is fault tolerant, 
distributed and can work with non-trusted servers due to implementation of cryptographic data authenticity.

[Global-FS CLI](https://github.com/softindex/datakernel/tree/master/examples/global-fs-cli)

[Global-FS Demo](https://github.com/softindex/datakernel/tree/master/examples/global-fs-demo)

### [Global-OT](https://github.com/softindex/datakernel/tree/master/global-ot)

A framework which extends both OT technology and blockchain technology (representing the data as Git-like graph of 
individual immutable content-addressable commits, encrypted and signed with private key of its owner). Global-OT can 
work with non-trusted servers. 

[Global-OT Demo](https://github.com/softindex/datakernel/tree/master/examples/global-ot-demo)


### [Global-DB](https://github.com/softindex/datakernel/tree/master/global-db)

This module is a variation of Global-FS optimized for storing and synchronization small binary key-value pairs.

[Global-DB demo](https://github.com/softindex/datakernel/tree/master/examples/global-db-demo)

## Integration components

### [Boot](https://github.com/softindex/datakernel/tree/master/boot)

An intelligent way of booting complex applications and services according to their dependencies. Component contains 
several modules that help to easily configure and launch an application.

[Boot examples](https://github.com/softindex/datakernel/tree/master/examples/boot)

### [Launchers](https://github.com/softindex/datakernel/tree/master/launchers)

Module contains a set of predefined launchers as well as some common initializers for services. 
Contains standard launchers for CRDT, HTTP, FS and RPC modules.

[Launchers examples](https://github.com/softindex/datakernel/tree/master/examples/launchers)

### [UIKernel](https://github.com/softindex/datakernel/tree/master/uikernel)
This module represents integration with UIKernel.io JS frontend library: JSON serializers, grid model, basic servlets. 
Using this module you can build a server application that will be compatible with UIKernel JS library. 

[UIKernel integration example](https://github.com/softindex/datakernel/tree/master/examples/uikernel-integration)

## Getting started
There are three guides which demonstrate some basic yet extremely important features of DataKernel to get started with 
the framework:
1. ["Hello World!"](https://github.com/softindex/datakernel/tree/master/examples/getting-started) - this guide shows 
 how to create a simple “Hello World” application using Eventloop, which is the core component of DataKernel framework.
2. ["Hello World!" HTTP Server](https://github.com/softindex/datakernel/tree/master/examples/http-helloworld) - create a 
simple but scalable HTTP server using Boot and HTTP modules.
3. [Remote key-value storage](https://github.com/softindex/datakernel/tree/master/examples/remote-key-value-storage) - in 
this guide we will create a remote key-value storage with basic operations "put" and "get" utilizing Boot and RPC modules.