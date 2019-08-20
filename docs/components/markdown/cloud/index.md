---
is-index: true
id: cloud
nav-menu: cloud
layout: cloud
filename: cloud/
title: Cloud components of DataKernel Framework
status: STABLE
description: Technologies for building fully decentralized private cloud solutions and applications. Suitable for working with trusted servers only. 
---

| Components | Description | 
| --- | --- | 
|**[RPC](rpc.html)**| High-performance and fault-tolerant remote procedure call module for building distributed applications with an extremely efficient asynchronous binary RPC streaming protocol. | 
|**[FS](fs.html)**| Basis for building scalable remote file storage with implementation of caching and fast asynchronous file I/O based on Java NIO. Utilizes CSP for fast and reliable file transfer. | 
|**[OT](ot.html)**| This module allows to build collaborative software systems based on Git-like approach combined with automatic conflict resolution, utilizing a special algorithm for operational transformations.|
|**[LSM Tree Aggregation](aggregation.html)**| Log-structured merge-tree table which stores aggregate functions and designed for OLAP workload.|
|**[LSM Tree OLAP Cube](cube.html)**|Multidimensional OLAP (Online Analytical Processing) database with predefined set of dimensions, measures, and log-structured merge-tree tables containing pre-aggregated data. LSM Tree database efficiently executes multi-dimensional analytical queries.|
|**[Dataflow](dataflow.html)**| Distributed stream-based batch processing engine for Big Data applications. Contains tools to work with data sets which can span multiple partitions. |
|**[CRDT](crdt.html)**| Conflict-free replicated data type implementation (specifically, state-based CRDT). Contains tools to create collaborative editing applications using CRDT approach to merge data that comes from multiple nodes into a single CRDT structure.| 
|**[Multilog](multilog.html)**| This module manages integrity of log files stored in distributed file system and allows to work with them as if they were stored in a single place.|
|**[ETL](etl.html)**| Processes logs using operational transformations. Uses OT module to persist logs and resolve conflicts. |

