**Website:** http://datakernel.io

**DataKernel Java framework** allows to build scalable, extremely fast and resilient network applications. It uses unified **'Event Loop'** architecture
and allows to build everything from asynchronous I/O operations on standalone servers to efficient Big Data cluster solutions.

# Foundation Components

## Eventloop
Eventloop module is the foundation of other modules that run their code inside event loops and threads. Useful for building client-server applications with high performance requirements.

* Node.js-like approach for asynchronous I/O (TCP, UDP)
* Eliminates traditional bottleneck of I/O for further business logic processing
* Can run multiple event loop threads on available cores
* Minimal GC pressure: arrays and byte buffers are reused
* Integration with Guice dependency injection
* Service graph DAGs allows to start and stop services dependent on each other, concurrently and in correct order

# Core Components

## HTTP

HTTP module enables users to build HTTP servers and clients that take full advantage of asynchronous I/O.

* HTTP Server - ideal for web services which require async I/O (like using RPC or calling other web services for serving requests)
* HTTP Client - ideal for high-performance clients of web services with a large number of parallel HTTP requests
* [up to ~238K of requests per second per core](http://datakernel.io/docs/http/#benchmark)
* ~50K of concurrent HTTP connections
* Low GC pressure
* Built on top of [Eventloop](#eventloop) module

## Async Streams

Async Streams module is useful for intra- and inter-server communication and asynchronous data processing.

* Modern implementation of async reactive streams (unlike streams in Java 8 and traditional thread-based blocking streams)
* Asynchronous with extremely efficient congestion control, to handle natural imbalance in speed of data sources
* Composable stream operations (mappers, reducers, filters, sorters, mergers/splitters, compression, serialization)
* Stream-based network and file I/O on top of [Eventloop](#eventloop) module

## Serializer

Serializers module is a bytecode generator of extremely fast and space efficient serializers, for transferring data over wire or persisting it into a file system or database.

* Schema-less approach - for maximum performance and compactness (unlike other serializers, there is no overhead in typed values)
* Implemented using runtime bytecode generation, to be compatible with dynamically created classes (like intermediate POJOs created with [Codegen](#codegen) module)

## Codegen

Codegen module allows to build classes and methods in runtime without the overhead of reflection.

* Dynamically creates classes needed for runtime query processing (storing the results of computation, intermediate tuples, compound keys etc.)
* Implements basic relational algebra operations for individual items: aggregate functions, projections, predicates, ordering, group-by etc.
* Since I/O overhead is already minimal due to [Eventloop](#eventloop) module, bytecode generation ensures that business logic (such as innermost loops processing millions of items) is also as fast as possible
* Easy to use API that encapsulates most of the complexity involved in working with bytecode

# Cluster Components

## RPC

RPC module is the framework to build distributed applications requiring efficient client-server interconnections between servers.

* Ideal to create near-realtime (i.e. memcache-like) servers with application-specific business logic
* **[Up to ~5.7M of requests per second on single core](http://datakernel.io/docs/rpc/#benchmark)**
* Pluggable high-performance asynchronous binary RPC streaming protocol
* Consistent hashing and round-robin distribution strategies
* Fault tolerance - with reconnections to fallback and replica servers

## Cube

Cube module enables near-real time reporting for multidimensional data streams by \"pre-aggregrating\" streams into specified sets.

* Log-Structured Merge Trees as core storage principle for its aggregations (unlike OLTP databases, it is designed from ground up for OLAP workload)
* Up to ~1.5M of inserts per second into aggregation on single core
* **Live** OLAP queries with incremental updates
* Aggregations storage medium can use any distributed file system
* Query API exposed through JSON HTTP (for interoperability with JS web clients) and serialized async streams (for maximum performance)
* Uses [Eventloop](#eventloop) for fast log processing I/O, [Async Streams](#async-streams) and [Serializers](#serializer) for aggregations and logs processing, [Codegen](#codegen) for aggregate functions and group-by operations

## Datagraph

Datagraph module is the foundation for building big-data batch processing applications that run on cluster.

* Notion of distributed streams: abstraction over physical data streams, their physical locations and partitioning
* Distributed stream operators can be composed with simple DSL syntax (mappers, reducers, filters, joiners, sorters, iterative batch processing etc.)
* Composed computation is automatically compiled into a distributed execution plan, which streams partitions of actual data between physical nodes, and arranges parallel computations on computing nodes
* Uses [Eventloop](#eventloop) for fast async I/O, [Async Streams](#async-streams) and [Serializers](#serializer) for data transfers and processing, [Codegen](#codegen) for fast operations on individual data items

## SimpleFS

SimpleFS module contains a simple, yet very efficient, single-node file server.

* Straightforward to use
* Lightweight
* Fast and efficient through the use of non-blocking eventloop-based network and file I/O

## HashFS

The HashFS module provides a distributed, decentralised and fault-tolerant file server.

Disruptions due to a node failure are minimal because of a smart file redistribution
implemented using a <a href="https://en.wikipedia.org/wiki/Rendezvous_hashing">rendezvous hashing algorithm</a>.

* Replication: File is kept replicated across the constant number of nodes
* Rebalancing: When node fails, the files stored on it are moved to other nodes
* Uniform load distribution across nodes
