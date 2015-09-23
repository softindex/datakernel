## Introduction

DataKernel is an open-source Java framework, which is comprised of a collection of components useful for creating high-load server systems. For instance, [benchmarks show](http://datakernel.io/docs/http/#benchmark) that our HTTP-server outperforms nginx (with a default configuration), in some cases delivering twice as many requests per second.

The essential components of DataKernel forms the basis of our ad-serving infrastructure at [AdKernel](http://adkernel.com), running in production environments and processing billions of requests. Specifically, DataKernel is the foundation for systems that provide real-time analytics for ad publishers and advertisers, user data processing tools that we use for ad targeting, and also web crawlers that perform content indexing on a large scale.

## Foundation components

* [Eventloop](http://datakernel.io/docs/eventloop/) - Efficient non-blocking network and file I/O, for building Node.js-like client/server applications with high performance requirements.

## Core components

* [HTTP](http://datakernel.io/docs/http/) - High-performance asynchronous HTTP client and server. [Benchmark](http://datakernel.io/docs/http/#benchmark)
* [Async Streams](http://datakernel.io/docs/streams/) - Composable asynchronous/reactive streams with powerful data processing capabilities. [Benchmark](http://datakernel.io/docs/streams/#benchmark)
* [Serializer](http://datakernel.io/docs/serializers/) - Extremely fast and space-efficient serializers, crafted using bytecode engineering. [Benchmark](http://datakernel.io/docs/serializers/#benchmark)
* [Codegen](http://datakernel.io/docs/codegen/) - Expression-based fluent API on top of ObjectWeb ASM for runtime generation of POJOs, mappers and reducers, etc.

## Cluster components

* [RPC](http://datakernel.io/docs/rpc/) - High-performance and fault-tolerant remote procedure call module for building distributed applications. [Benchmark](http://datakernel.io/docs/rpc/#benchmark)
* [Cube](http://datakernel.io/docs/cube/) - Specialized OLAP database for multidimensional data analytics.
* [Datagraph](http://datakernel.io/docs/datagraph/) - Distributed stream-based batch processing engine for Big Data applications.
* [SimpleFS](http://datakernel.io/docs/simplefs/) - Simple, yet very efficient, single-node file server. [Benchmark](http://datakernel.io/docs/streams/#simplefs)
* [HashFS](http://datakernel.io/docs/hashfs/) - Distributed fault-tolerant low-overhead file server with automatic replication and resharding.
